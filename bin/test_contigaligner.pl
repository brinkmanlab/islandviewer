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
use Islandviewer::ContigAligner;
use Islandviewer::NullCallback;

MAIN: {
    my $cfname; my $workdir; my $accnum; my $logger;
    my $ref_accnum; my $microbedb;
    # accnum would here be the cid of a genome previously submitted to islandviewer
    # (since microbedbv2 only retrieves complete genomes, it does not contain any draft genomes for testing)
    my $res = GetOptions("config=s"   => \$cfname,
			 "microbedb=s" => \$microbedb,
			 "ref_accnum=s" => \$ref_accnum,
			 "workdir=s"  => \$workdir,
			 "accnum=s" => \$accnum,
    );

    die "Error, no config file given"
      unless($cfname);

    my $Islandviewer = Islandviewer->new({cfg_file => $cfname });
    my $cfg = Islandviewer::Config->config;

    my $callback_obj = Islandviewer::NullCallback->new();

    if($cfg->{logger_conf} && ( -r $cfg->{logger_conf})) {
	Log::Log4perl::init($cfg->{logger_conf});
	$logger = Log::Log4perl->get_logger;
	$logger->debug("Logging initialize");
	# We want to ensure trace level for an update
	$logger->level("TRACE");
    }

    my $aligner_obj = Islandviewer::ContigAligner->new(
	{ workdir => $workdir,
	  microbedb_ver => $microbedb,
	  ref_accnum => $ref_accnum });

    my $res = $aligner_obj->run($accnum, $callback_obj);

    print "Run status: $res\n";
}

