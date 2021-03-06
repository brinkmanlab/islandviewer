#!/usr/bin/env perl

use strict;
use Cwd qw(abs_path getcwd);
use Getopt::Long;
use Data::Dumper;
use File::Spec::Functions;

BEGIN{
# Find absolute path of script
my ($path) = abs_path($0) =~ /^(.+)\//;
chdir($path);
sub mypath { return $path; }
};

use lib "../lib";
use Islandviewer;
use Islandviewer::Analysis;

use MicrobedbV2::Singleton;

MAIN: {
    my $cfname; my $aid; my $module; my $logger;
    my $res = GetOptions("config=s"   => \$cfname,
			 "analyis=s" => \$aid,
			 "module=s"  => \$module,
    );

    umask 0022;

    die "Error, no config file given"
      unless($cfname);

    my $Islandviewer = Islandviewer->new({cfg_file => $cfname });
    my $cfg = Islandviewer::Config->config;

    if($cfg->{logger_conf} && ( -r $cfg->{logger_conf})) {
	Log::Log4perl::init($cfg->{logger_conf});
	$logger = Log::Log4perl->get_logger;

	my $app = Log::Log4perl->appender_by_name("errorlog");

	my $logpath = catdir($cfg->{analysis_directory}, $aid);
	if( -d $logpath ) {
	    $app->file_switch("$logpath/analysis.log");	    
	} else {
	    $app->file_switch($cfg->{analysis_log});
	}

	$logger->debug("Logging initialized, aid $aid, module $module");
	$logger->trace("Process umask is: " . umask);

    }

    my $microbedb = MicrobedbV2::Singleton->fetch_schema;
    my $dsn = MicrobedbV2::Singleton->fetch_dsn();
    $logger->trace("MicrobeDB dbh: $dsn");

    $Islandviewer->run($aid, $module);
};
