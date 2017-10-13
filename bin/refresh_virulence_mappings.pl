#!/usr/bin/env perl

# Refresh the virulence_mapped table for all
# the precomputed genomes.

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
use Islandviewer::AnnotationTransfer;

use MicrobedbV2::Singleton;

my $microbedb_ver;
my $logger;

MAIN: {
    my $cfname;
    my $res = GetOptions(
         "config=s" => \$cfname,
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

    my $find_analysis = $dbh->prepare("SELECT ext_id FROM Analysis WHERE owner_id = 0 AND default_analysis = 1 AND status = 4 AND atype = 2");

    $find_analysis->execute();

    my $transfer_obj = Islandviewer::AnnotationTransfer->new({microbedb_ver => $microbedb_ver, 
							   workdir => $cfg->{workdir} });


    while(my ($ext_id) = $find_analysis->fetchrow_array) {

        $logger->debug("Refreshing annotations for : $ext_id");

	my $results = $transfer_obj->run($ext_id);

	$logger->info("Annotations transferred from: " . Dumper($results));
   }
}
