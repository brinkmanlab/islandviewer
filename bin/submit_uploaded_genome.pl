#!/usr/bin/env perl

use warnings;
use strict;
use Cwd qw(abs_path getcwd);
use Getopt::Long;
use Date::Manip;
use File::Spec;

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

use MicrobedbV2::Singleton;

MAIN: {
    my $cfname; my $logger; my $logger_cfg;
    my $filename; my $genome_name; my $microbedb_ver;
    my $res = GetOptions("config=s" => \$cfname,
			 "filename=s" => \$filename,
			 "name=s" => \$genome_name,
			 "microbedb=s" => \$microbedb_ver,
			 "logger=s" => \$logger_cfg,
    );

    die "Error, no config file given"
      unless($cfname);

    my $Islandviewer = Islandviewer->new({cfg_file => $cfname });

    my $cfg = Islandviewer::Config->config;

    if($logger_cfg && ( -r $logger_cfg )) {
	Log::Log4perl::init($logger_cfg);
	$logger = Log::Log4perl->get_logger;
	$logger->debug("Logging initialize");
    } elsif($cfg->{logger_conf} && ( -r $cfg->{logger_conf})) {
	Log::Log4perl::init($cfg->{logger_conf});
	$logger = Log::Log4perl->get_logger;
	$logger->debug("Logging initialize");
    }

    my $datestr = UnixDate("now", "%Y%m%d");
    my $app = Log::Log4perl->appender_by_name("errorlog");
    if($cfg->{logdir}) {
	$app->file_switch(File::Spec->catpath(undef, $cfg->{logdir}, "custom_upload.log"));
    }
    $logger->info("Submitting genome $genome_name using file $filename");

    # Create a Versions object to look up the correct version
    my $microbedb = MicrobedbV2::Singleton->fetch_schema;

    # If we've been given a microbedb version AND its valid... 
    if($microbedb_ver && $microbedb->fetch_version($microbedb_ver)) {
	$microbedb_ver = $microbedb_ver;
    } else {
	$microbedb_ver = $microbedb->current();
    }

    unless($microbedb_ver) {
	$logger->logdie("Error, this should never happen, we don't seem to have a valid microbedb version: $microbedb_ver");
    }
    # We should have all the distances done now, let's do the IV

    my $cid = 0;
    eval {
	$cid = $Islandviewer->submit_and_prep($filename, $genome_name);
    };
    if($@) {
	$logger->logdie("Error submitting custom genome ($filename): $@");
    }
    unless($cid) {
	$logger->logdie("Error, didn't get a cid for custom genome ($filename)");
    }
    $logger->info("Submitted custom genome ($filename), cid $cid");

    # We're going to use the same arguments for all the runs
    my $args->{Islandpick} = {
			      MIN_GI_SIZE => 4000};
    $args->{Sigi} = {
			      MIN_GI_SIZE => 4000};
    $args->{Dimob} = {
			      MIN_GI_SIZE => 4000};
    $args->{Distance} = {block => 1, scheduler => 'Islandviewer::NullScheduler'};
    $args->{microbedb_ver} = $microbedb_ver;
    $args->{owner_id} = 1;
    $args->{email} = 'lairdm@sfu.ca';

    my $aid;
    eval {
	# Submit the replicon for processing
	$aid = $Islandviewer->submit_analysis($cid, $args);
    };
    if($@) {
	$logger->logdie("Error submitting analysis ($filename, $cid): $@");
    }
    if($aid) {
	$logger->info("Finished submitting $cid, has analysis id $aid");
    } else {
	$logger->logdie("Error, failed submitting, didn't get an analysis id");
    }

    $logger->info("All analysis should now be submitted");

    # Spit out the analysis id back for the web service
    print $aid;

}
