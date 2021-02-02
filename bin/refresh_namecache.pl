#!/usr/bin/env perl

use warnings;
use strict;
use Cwd qw(abs_path getcwd);
use Getopt::Long;
use Date::Manip;
use File::Spec;
use JSON;
use Data::Dumper;

BEGIN{
# Find absolute path of script
my ($path) = abs_path($0) =~ /^(.+)\//;
chdir($path);
sub mypath { return $path; }
};

use lib "../lib";
#use lib "/home/lairdm/libs";
use Islandviewer;
use Islandviewer::Config;
use Islandviewer::DBISingleton;
use Islandviewer::Distance;
use Islandviewer::Genome_Picker;

use MicrobedbV2::Singleton;

my $microbedb_ver;
my $logger;

MAIN: {
    my $cfname;
    my $res = GetOptions("config=s" => \$cfname,
    );

    die "Error, no config file given"
      unless($cfname);

    my $Islandviewer = Islandviewer->new({cfg_file => $cfname });

    my $cfg = Islandviewer::Config->config;

    if($cfg->{logger_conf} && ( -r $cfg->{logger_conf})) {
	Log::Log4perl::init($cfg->{logger_conf});
	$logger = Log::Log4perl->get_logger;
	$logger->debug("Logging initialize");
	# We want to ensure trace level for an update
	$logger->level("TRACE");
    }

    my $datestr = UnixDate("now", "%Y%m%d");
    my $app = Log::Log4perl->appender_by_name("errorlog");
    if($cfg->{logdir}) {
	$app->file_switch($cfg->{logdir} . "/ivupdate.$datestr.log");
    }

    # Get the DB handles
    my $dbh = Islandviewer::DBISingleton->dbh;
    my $microbedb = MicrobedbV2::Singleton->fetch_schema;

    # What is the current version of microbedb?
    $microbedb_ver = $microbedb->current();
    $logger->info("Using microbedb version $microbedb_ver");

    my $find_names = $dbh->prepare("SELECT id, cid FROM NameCache");
    my $invalidate_genome = $dbh->prepare("UPDATE NameCache SET isvalid = 0 WHERE id = ?");
    my $validate_genome = $dbh->prepare("UPDATE NameCache SET isvalid = 1 WHERE id = ?");

    $find_names->execute();

    while(my ($id, $cid) = $find_names->fetchrow_array) {


        $logger->debug("Checking record: $cid ($id)");

        # See if it's in the current version of MicrobeDB
        my $rep_results = $microbedb->resultset('Replicon')->search( {
            rep_accnum => $cid,
            version_id => $microbedb_ver
                                                                     }
            )->first;

        if( defined($rep_results) ) {
            my $base_file = File::Spec->catpath(undef, $rep_results->genomeproject->gpv_directory, $rep_results->file_name);
            $logger->debug("Looking for a valid genome with base: $base_file");

            if(-f "$base_file.gbk" && -f "$base_file.faa" && "$base_file.fna" && "$base_file.ptt") {
                $logger->info("Found $cid, great!");
                $validate_genome->execute($id);
                next;
            } else {
                $logger->error("We found a DB entry but not a file for $cid??? $base_file");
            }
        }

        $logger->info("Can't seem to find a valid genome for $cid ($id), invalidating");
        $invalidate_genome->execute($id);
    }
}
