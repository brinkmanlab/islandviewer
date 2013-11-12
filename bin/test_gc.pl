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
use Islandviewer::GenomeUtils;


MAIN: {
    my $cfname; my $workdir; my $filename;
    my $res = GetOptions("config=s"   => \$cfname,
			 "workdir=s"  => \$workdir,
			 "filename=s" => \$filename,
    );

    die "Error, no config file given"
      unless($cfname);

    my $Islandviewer = Islandviewer->new({cfg_file => $cfname });
    my $cfg = Islandviewer::Config->config;

    my $genome_obj = Islandviewer::Islandpick::GenomeUtils->new(
	{ workdir => $workdir});

    my($length, $min, $max, $mean, @gc_values) = $genome_obj->calculate_gc($filename);

    print "$length, $min, $max, $mean\n";
    print "var gc_values=[" . (join ',', @gc_values);
    print  "]\n";

}
