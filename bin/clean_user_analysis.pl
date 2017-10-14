#!/usr/bin/env perl

# Clean a given user analysis from the database and scrub associated files
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
#use lib "/home/lairdm/libs";
use Islandviewer;
use Islandviewer::Config;
use Islandviewer::DBISingleton;

my $cfname; my $logger; my $token; my $cfg;

MAIN: {
    my $res = GetOptions("config=s" => \$cfname,
                         "token=s" => \$token,
    );

    die "Error, no config file given"
      unless($cfname);

    die "Error, no token given" 
      unless($token);

    my $Islandviewer = Islandviewer->new({cfg_file => $cfname });

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

    $logger->info("Purging analysis $token for user");

    my $dbh = Islandviewer::DBISingleton->dbh;
    my $find_old_custom = $dbh->prepare("SELECT aid, ext_id, workdir FROM Analysis WHERE atype = 1 AND owner_id != 0 AND token = '$token'"); 
    $find_old_custom->execute();

    while(my @row = $find_old_custom->fetchrow_array) {

        my $aid = $row[0];
        my $ext_id = $row[1];
        $logger->info("Purging analysis $aid, ext_id $ext_id");

        my $full_path = Islandviewer::Config->expand_directory($row[2]);

        purge_old_customgenome($ext_id);
        purge_old_custom_analysis($full_path, $aid, $ext_id);
        purge_old_uploadgenome($ext_id);
    }

    $logger->info("Done purge");

    exit;
}

sub purge_old_custom_analysis {

    my $full_path=shift;
    my $aid=shift;
    my $ext_id=shift;

    if(-d $full_path) {
        $logger->info("Removing analysis path $full_path");
	remove_tree($full_path);
    }

    my $dbh = Islandviewer::DBISingleton->dbh;

    # Remove all the db references
    $dbh->do("DELETE FROM IslandGenes WHERE gi IN (SELECT gi FROM GenomicIsland WHERE aid_id = ?)", undef, $aid);
    $dbh->do("DELETE FROM Genes WHERE ext_id = ?", undef, $ext_id);
    $dbh->do("DELETE FROM GenomicIsland WHERE aid_id = ?", undef, $aid);
    $dbh->do("DELETE FROM GIAnalysisTask WHERE aid_id = ?", undef, $aid);
    $dbh->do("DELETE FROM Notification WHERE analysis_id = ?", undef, $aid);
    $dbh->do("DELETE FROM Analysis WHERE aid = ?", undef, $aid);

}


sub purge_old_uploadgenome {

    my $ext_id=shift;

    my $dbh = Islandviewer::DBISingleton->dbh;

    my $find_old_uploadgenome = $dbh->prepare("SELECT id, filename from UploadGenome WHERE cid=$ext_id");

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

    my $ext_id=shift;

    $logger->info("Purging custome genome " . $ext_id);

    my $custom_path = $cfg->{custom_genomes} . '/' . $ext_id;

    if(-d $custom_path) {
        $logger->info("Removing custom genome directory $custom_path");
        remove_tree($custom_path);
    }

    my $dbh = Islandviewer::DBISingleton->dbh;

    $dbh->do("DELETE FROM Distance WHERE rep_accnum1 = ? OR rep_accnum2 = ?", undef, $ext_id, $ext_id);
    $dbh->do("DELETE FROM GC WHERE ext_id = ?", undef, $ext_id);
    $dbh->do("DELETE FROM CustomGenome WHERE cid = ?", undef, $ext_id);
}
