#!/usr/bin/env perl

# Clean older analysis from the database and scrub associated
# files
#
# Yes, it works.


use warnings;
use strict;
use Cwd qw(abs_path getcwd);
use Getopt::Long;
use Date::Manip;
use File::Spec::Functions;
use File::Path qw(remove_tree);
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

my $cfname; my $logger; my $maxage; my $cfg;

MAIN: {
    my $res = GetOptions("config=s" => \$cfname,
                         "maxage=s" => \$maxage,
    );

    die "Error, no config file given"
      unless($cfname);

    my $Islandviewer = Islandviewer->new({cfg_file => $cfname });

    unless($maxage =~ /\d+/) {
        $maxage = 90;
    }

    $cfg = Islandviewer::Config->config;

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
        $app->file_switch(File::Spec->catpath(undef, $cfg->{logdir}, "ivpurge.$datestr.log"));
    }

    $logger->info("Purging analysis older than $maxage days");

    purge_old_custom_analysis();
    purge_old_uploadgenome();
    purge_old_customgenome();
    purge_old_rerun_analysis();

    $logger->info("Done purge");

    exit;
}

sub purge_old_custom_analysis {
    my $dbh = Islandviewer::DBISingleton->dbh;

    my $find_old_custom = $dbh->prepare("SELECT aid, ext_id, workdir FROM Analysis WHERE atype = 1 AND owner_id != 0 AND DATE_SUB(CURDATE(), INTERVAL $maxage DAY) >= start_date");

    $find_old_custom->execute();

    while(my @row = $find_old_custom->fetchrow_array) {
        # We want to:
        # - purge the Analysis directory
        # - Remove the DB entries

        my $aid = $row[0];
        $logger->info("Purging analysis $aid, ext_id " . $row[1]);

        my $full_path = Islandviewer::Config->expand_directory($row[2]);

        if(-d $full_path) {
            $logger->info("Removing analysis path $full_path");
	    remove_tree($full_path);
        }

        # Remove all the db references
        $dbh->do("DELETE FROM IslandGenes WHERE gi IN (SELECT gi FROM GenomicIsland WHERE aid_id = ?)", undef, $aid);
        $dbh->do("DELETE FROM GenomicIsland WHERE aid_id = ?", undef, $aid);
        $dbh->do("DELETE FROM GIAnalysisTask WHERE aid_id = ?", undef, $aid);
        $dbh->do("DELETE FROM Notification WHERE analysis_id = ?", undef, $aid);
        $dbh->do("DELETE FROM Analysis WHERE aid = ?", undef, $aid);
    }

}

sub purge_old_rerun_analysis {
    my $dbh = Islandviewer::DBISingleton->dbh;

    my $find_old_custom = $dbh->prepare("SELECT aid, ext_id, workdir FROM Analysis WHERE default_analysis = 0 AND DATE_SUB(CURDATE(), INTERVAL $maxage DAY) >= start_date");

    $find_old_custom->execute();

    while(my @row = $find_old_custom->fetchrow_array) {
        # We want to:
        # - purge the Analysis directory
        # - Remove the DB entries

        my $aid = $row[0];
        $logger->info("Purging non-default analysis $aid, ext_id " . $row[1]);

        my $full_path = Islandviewer::Config->expand_directory($row[2]);

        if(-d $full_path) {
            $logger->info("Removing analysis path $full_path");
	    remove_tree($full_path);
        }

        # Remove all the db references
        $dbh->do("DELETE FROM IslandGenes WHERE gi IN (SELECT gi FROM GenomicIsland WHERE aid_id = ?)", undef, $aid);
        $dbh->do("DELETE FROM GenomicIsland WHERE aid_id = ?", undef, $aid);
        $dbh->do("DELETE FROM GIAnalysisTask WHERE aid_id = ?", undef, $aid);
        $dbh->do("DELETE FROM Notification WHERE analysis_id = ?", undef, $aid);
        $dbh->do("DELETE FROM Analysis WHERE aid = ?", undef, $aid);
    }

}

sub purge_old_uploadgenome {
    my $dbh = Islandviewer::DBISingleton->dbh;

    my $find_old_uploadgenome = $dbh->prepare("SELECT id, filename from UploadGenome WHERE DATE_SUB(CURDATE(), INTERVAL $maxage DAY) >= date_uploaded");

    $find_old_uploadgenome->execute();

    while(my @row = $find_old_uploadgenome->fetchrow_array) {
        $logger->info("Purging uploaded genome " . $row[0]);

        if(-f $row[1]) {
            $logger->info("Removing uploaded file " . $row[1]);
            remove_tree($row[1]);
        }

        $dbh->do("DELETE FROM UploadGenome WHERE id = ?", undef, $row[0]);
    }
}

sub purge_old_customgenome {
    my $dbh = Islandviewer::DBISingleton->dbh;

    my $find_old_customgenome = $dbh->prepare("SELECT cid, filename from CustomGenome WHERE DATE_SUB(CURDATE(), INTERVAL $maxage DAY) >= submit_date");

    $find_old_customgenome->execute();

    while(my @row = $find_old_customgenome->fetchrow_array) {
        $logger->info("Purging custome genome " . $row[0]);

        my $custom_path = $cfg->{custom_genomes} . '/' . $row[0];

        if(-d $custom_path) {
            $logger->info("Removing custom genome directory $custom_path");
            remove_tree($custom_path);
        }

        $dbh->do("DELETE FROM Distance WHERE rep_accnum1 = ? OR rep_accnum2 = ?", undef, $row[0], $row[0]);
        $dbh->do("DELETE FROM GC WHERE ext_id = ?", undef, $row[0]);
        $dbh->do("DELETE FROM Genes WHERE ext_id = ?", undef, $row[0]);
        $dbh->do("DELETE FROM CustomGenome WHERE cid = ?", undef, $row[0]);
    }
}
