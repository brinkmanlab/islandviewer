#!/usr/bin/env perl

use warnings;
use strict;
use Cwd qw(abs_path getcwd);
use Getopt::Long;
use Date::Manip;
use File::Spec::Functions;

BEGIN{
# Find absolute path of script
my ($path) = abs_path($0) =~ /^(.+)\//;
chdir($path);
sub mypath { return $path; }
};

use lib "../lib";
use lib "/home/lairdm/libs";
use Islandviewer;
use Islandviewer::Config;
use Islandviewer::DBISingleton;
use Islandviewer::Distance;

use MicrobeDB::Search;
use MicrobeDB::Replicon;

MAIN: {
    my $cfname; my $logger;
    my $res = GetOptions("config=s" => \$cfname
    );

    die "Error, no config file given"
      unless($cfname);

    my $Islandviewer = Islandviewer->new({cfg_file => $cfname });

    my $cfg = Islandviewer::Config->config;

    if($cfg->{logger_conf} && ( -r $cfg->{logger_conf})) {
	Log::Log4perl::init($cfg->{logger_conf});
	$logger = Log::Log4perl->get_logger;
	$logger->debug("Logging initialize");
    }

    my $datestr = UnixDate("now", "%Y%m%d");
    my $app = Log::Log4perl->appender_by_name("errorlog");
    if($cfg->{logdir}) {
	$app->file_switch($cfg->{logdir} . "/ivupdate.$datestr.log");
    }

    my $base_work_dir = catdir($cfg->{workdir},"$datestr");
    $logger->debug("Making working directory for distance $base_work_dir");
    if( -d $base_work_dir) {
	$logger->logdie("Error, workdir already exists for today, not proceeding");
    }
    mkdir $base_work_dir;

    my $microbedb_ver; my $sets_run;
    my $sets_run_last_cycle = 99999999;
    my $cycle_num = 1;

    # We're going to loop until we stop computing more distances,
    # this will catch dying children that might cause some of our
    # distances to not be caught
    while(1) {
	eval{
	    # We need the trailing slash becauce the code that uses this expects
	    # it, my bad...
	    my $cycle_workdir =  catdir($base_work_dir, "cycle$cycle_num") . '/';
	    $logger->debug("Making workdir for cycle $cycle_num: $cycle_workdir");
	    mkdir $cycle_workdir;

	    my $dist_obj = Islandviewer::Distance->new({scheduler => 'Islandviewer::Torque', workdir => $cycle_workdir, num_jobs => 200, block => 1 });

	    ($microbedb_ver,$sets_run) = $dist_obj->calculate_all();
	};
	if($@) {
	    die "Error updating islandviewer in distance phase: $@";
	}

	if($sets_run == 0) {
	    $logger->info("No sets to run, moving on...");
	    last;
	} elsif($sets_run < $sets_run_last_cycle) {
	    $logger->info("We ran $sets_run this attempt, $sets_run_last_cycle last time");
	} elsif($sets_run == $sets_run_last_cycle) {
	    # This can either be if its stuck not getting more or if it hits zero
	    $logger->info("We ran the same number of sets as last cycle ($sets_run), moving on...");
	    last;
	} else {
	    $logger->logdie("Something really weird happened, this cycle: $sets_run, last cycle: $sets_run_last_cycle");
	}

	$sets_run_last_cycle = $sets_run;
    }
    unless($microbedb_ver) {
	die "Error, this should never happen, we don't seem to have a valid microbedb version: $microbedb_ver";
    }

    # We should have all the distances done now, let's do the IV
    my $so = new MicrobeDB::Search();

    # Find all the replicons in this version
    my @reps = $so->object_search(new MicrobeDB::Replicon(version_id => $microbedb_ver, rep_type=>'chromosome'));

    my $dbh = Islandviewer::DBISingleton->dbh;
    my $check_analysis = $dbh->prepare("SELECT aid, microbedb_ver FROM Analysis WHERE ext_id = ? and default_analysis = 1");

    # We're going to use the same arguments for all the runs
    my $args->{Islandpick} = {
			      MIN_GI_SIZE => 4000};
    $args->{Sigi} = {
			      MIN_GI_SIZE => 4000};
    $args->{Dimob} = {
			      MIN_GI_SIZE => 4000};
    $args->{Distance} = {block => 1, scheduler => 'Islandviewer::NullScheduler'};
    $args->{microbedb_ver} = $microbedb_ver;
    $args->{default_analysis} = 1;
    $args->{email} = 'lairdm@sfu.ca';

my $count = 0;

    foreach my $curr_rep (@reps) {
	my $accnum = $curr_rep->rep_accnum();

	# Has this replicon already been run before?
	$check_analysis->execute($accnum);
	if(my @row = $check_analysis->fetchrow_array) {
	    $logger->info("We already have $accnum in the database as analysis $row[0]");
	    next;
	} else {
	    # Else its new so add it to the name cache
	    $dbh->do("INSERT IGNORE INTO NameCache (cid, name) VALUES (?, ?)", undef,
		     $accnum,
		     $curr_rep->definition()
		) or $logger->logdie("Error inserting in to NameCache for $accnum, " . $curr_rep->definition());
	}
	
	# Submit the replicon for processing
	my $aid = 0;
	my $starttime = time;
	eval {
	    $aid = $Islandviewer->submit_analysis($accnum, $args);
	};
	if($@) {
	    $logger->error("Error submitting analysis $accnum: $@");
	}
	if($aid) {
	    $logger->debug("Finished submitting $accnum, has analysis id $aid");
	} else {
	    $logger->error("Error submitting $accnum, didn't get an aid");
	}
	if($count > 250) {
	    last;
	}
	$count++;
	my $diff = time - $starttime;
	# We don't want to submit too quickly....
	if($diff < 5) {
	    sleep abs(5 - $diff);
	}

    }

    $logger->info("All analysis should now be submitted");
}
