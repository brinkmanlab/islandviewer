=head1 NAME

    Islandviewer::NullScheduler

=head1 DESCRIPTION

    Run jobs using a straight system() call
    using a & to background the process.
    This module does NOT block for the system
    call.

=head1 SYNOPSIS

    use Islandviewer::NullScheduler;

    my $scheduler = Islandviewer::NullScheduler->new();
    $scheduler->submit($cmd);

=head1 AUTHOR

    Matthew Laird
    Brinkman Laboratory
    Simon Fraser University
    Email: lairdm@sfu.ca

=head1 LAST MAINTAINED

    Dec 29, 2016

=cut

package Islandviewer::NullScheduler;

use strict;
use File::chdir;
use Moose;
use Log::Log4perl qw(get_logger :nowarn);

use Islandviewer::Config;

my $cfg; my $cfg_file; my $logger;

sub BUILD {
    my $self = shift;
    my $args = shift;

    $cfg = Islandviewer::Config->config;
    $cfg_file = File::Spec->rel2abs(Islandviewer::Config->config_file);

    $logger = Log::Log4perl->get_logger;

}

sub submit {
    my $self = shift;
    my $name = shift;
    my $cmd = shift;
    my $workdir = shift;

    $logger->debug("Changing cwd before call");

    local $CWD = $workdir;

    $logger->debug("Making system call $cmd");

    my $ret = system("$cmd&");

    if($ret) {
	# Non-zero return value, bad...
	$logger->error("Error making system call $cmd");
	return 0;
    }

    return 1;

}

# We're going to assume the modules come in the order they
# should be executed.

sub build_and_submit {
    my $self = shift;
    my $aid = shift;
    my $job_type = shift;
    my $workdir = shift;
    my $args = shift;
    my @modules = @_;

    $logger->debug("Building linear job for analysis $aid");

    $logger->debug("Changing cwd for call");

    local $CWD = $workdir;

    my $script_name = "$workdir/islandviewer_job.sh";

    # Open the script for the set of commands
    open(SCRIPT, ">$script_name")
	or $logger->logdie("Error creating islandviewer run script $script_name for $aid, $@");

    print SCRIPT "# Islandviewer job script for aid $aid\n\n";

    # We need to set the environment variable for the 
    # MicrobeDB API, so it knows what database to connect to
    my $microbedb_database;
    if($cfg->{microbedb}) {
        $microbedb_database = $cfg->{microbedb};
    } elsif($ENV{"MicrobeDBV2"}) {
        $microbedb_database = $ENV{"MicrobeDBV2"};
    } elsif($ENV{"MicrobeDB"}) {
        $microbedb_database = $ENV{"MicrobeDB"};
    }

    if($microbedb_database) {
        print SCRIPT "# Setting MicrobeDB database to use\n";
        print SCRIPT "export MicrobeDB=\"$microbedb_database\"\n\n";
    }

    print SCRIPT "echo \"Starting analysis job $aid\"\n\n";

    foreach my $component (@modules) {
	my $cmd = $cfg->{component_runner} . " -c $cfg_file -a $aid -m $component";

	print SCRIPT "echo \"Starting component $component, command: $cmd\"\n";
	print SCRIPT "$cmd\n\n";
    }

    close SCRIPT;

    $logger->info("Submitting islandviewer script for $aid");
    
    my $ret = 0;
#    my $ret = system("sh $script_name&");

    if($ret) {
	# Non-zero return value, bad...
	$logger->error("Error making system call $script_name");
	return 0;
    }

    return 1;

}

1;
