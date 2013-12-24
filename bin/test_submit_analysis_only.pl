#!/usr/bin/env perl

use strict;
use Cwd qw(abs_path getcwd);
use Getopt::Long;
use Data::Dumper;

BEGIN{
# Find absolute path of script
my ($path) = abs_path($0) =~ /^(.+)\//;
chdir($path);
sub mypath { return $path; }
};

use lib "../lib";
use Islandviewer;

MAIN: {
    my $cfname; my $cid; my $logger;
    my $res = GetOptions("config=s"   => \$cfname,
			 "id=s" => \$cid,
    );

    die "Error, no config file given"
      unless($cfname);

    my $Islandviewer = Islandviewer->new({cfg_file => $cfname });
    my $cfg = Islandviewer::Config->config;

    if($cfg->{logger_conf} && ( -r $cfg->{logger_conf})) {
	Log::Log4perl::init($cfg->{logger_conf});
	$logger = Log::Log4perl->get_logger;
	$logger->debug("Logging initialized");
    }

    print "cid: $cid\n";

#    my $args->{Islandpick} = {comparison_genomes => "NC_11111 NC_22222",
    my $args->{Islandpick} = {
			      MIN_GI_SIZE => 4000};
    $args->{Distance} = {block => 1, scheduler => 'Islandviewer::NullScheduler'};
    $args->{microbedb_ver} = 81;
    $args->{email} = 'lairdm@sfu.ca';
    print Dumper $args;

    my $aid = $Islandviewer->submit_analysis($cid, $args);

    print "aid: $aid\n";

};
