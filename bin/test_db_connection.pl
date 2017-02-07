#!/usr/bin/env perl

use strict;
use Cwd qw(abs_path getcwd);
use Getopt::Long;
use DBI;


BEGIN{
# Find absolute path of script
my ($path) = abs_path($0) =~ /^(.+)\//;
chdir($path);
sub mypath { return $path; }
};

use lib "../lib";
use Islandviewer;
use Islandviewer::Schema;

MAIN: {
    my $cfname;
    my $res = GetOptions(
                   "config|c=s" => \$cfname, 
    );

    die "Error, no config file given"
      unless($cfname);

    my $Islandviewer = Islandviewer->new({cfg_file => $cfname });
    my $cfg = Islandviewer::Config->config;

    print $cfg->{dsn} . "\n";
 
    my $dbh = Islandviewer::Schema->connect($cfg->{dsn},
                           $cfg->{dbuser},
                           $cfg->{dbpass})
     or die "Error, can't connect to IslandViewer via DBIx";    
#    my $dbh = DBI->connect($cfg->{dsn},
#			   $cfg->{dbuser},
#			   $cfg->{dbpass});
#    die "Error: Unable to connect to the database: $DBI::errstr\n" if ! $dbh;
    
}
