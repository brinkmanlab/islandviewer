=head1 NAME

    Islandviewer::MetaScheduler

=head1 DESCRIPTION

    Submit jobs to MetaScheduler

=head1 SYNOPSIS

    use Islandviewer::MetaScheduler;

    my $scheduler = Islandviewer::MetaScheduler->new();
    $scheduler->submit($cmd);

=head1 AUTHOR

    Matthew Laird
    Brinkman Laboratory
    Simon Fraser University
    Email: lairdm@sfu.ca

=head1 LAST MAINTAINED

    Sept 27, 2013

=cut

package Islandviewer::MetaScheduler;

use strict;
use Moose;
use Log::Log4perl qw(get_logger :nowarn);
use JSON;
use Data::Dumper;

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

    # First let's make sure we have a work directory for qsub files
    mkdir "$cfg->{workdir}/qsub"
	unless( -d "$cfg->{workdir}/qsub" );

    open(QSUB, ">$cfg->{workdir}/qsub/$name.qsub") or
	die "Error, can't open qsub file $cfg->{workdir}/qsub/$name.qsub: $!";

    print QSUB "# Build by Islandviewer::MetaScheduler\n\n";
    print QSUB "echo \"Running cvtree for set $name\"\n";
    print QSUB "$cmd\n";

    close QSUB;

    
}

sub build_and_submit {
    my $self = shift;
    my $aid = shift;
    my $job_type = shift;
    my $workdir = shift;
    my $args = shift;
    my @modules = @_;

    $logger->debug("Building metascheduler submission for analysis $aid");

    # Now we need to start making our metascheduler job file
    my $meta_job->{job_name} = "IV $aid";
    $meta_job->{job_id} = $aid;
    $meta_job->{job_type} = $job_type;
    $meta_job->{job_scheduler} = 'Torque';
    $meta_job->{job_email} = [{'email' => $args->{email}}] if($args->{email});
    
    # Now add the components
    foreach my $component (@modules) {
	my $c->{component_type} = $component;
	$c->{qsub_file} = "$workdir/$component.qsub";
	push @{$meta_job->{components}}, $c;

	# Now we need to make these qsub files...
	$self->create_qsub("$workdir/$component.qsub",
	    $cfg->{component_runner} . " -c $cfg_file -a $aid -m $component");
    }

    # Convert it to JSON and write it out
    my $JSONstr = to_json($meta_job, {pretty => 1});
    open(JSON, ">$workdir/metascheduler.job") 
	or $logger->logdie("Error creating metascheduler job file for $aid, $@");

    print JSON $JSONstr;
    close JSON;

    # And submit the job file to metascheduler...
    if($cfg->{metascheduler_cmd}) {
	my $cmd = "$cfg->{metascheduler_cmd} submit -i $workdir/metascheduler.job";
	$logger->debug("Issuing submit command: $cmd");
	my $res = `$cmd`;
	$logger->trace("From submitting analysis $aid: $res");

	my $response = from_json($res);

	unless($response->{code} eq '200') {
	    # Error!
	    $logger->logdie("Error submitting analysis to scheduler, received: $res");
	}

    } else {
	$logger->logdie("Error, no metascheduler submit command defined, can't submit");
    }

}

sub create_qsub {
    my $self = shift;
    my $qsub_file = shift;
    my $cmd = shift;

    open(QSUB, ">$qsub_file") 
	or $logger->logdie("Error creating qsub file $qsub_file: $@");

    print QSUB "# qsub file for Islandviewer\n\n";
    
    # We need to set the correct home environment to allow various IslandViewer 
    # versions to run nicely alongside each other
    
    my $env_var = "";
    if($cfg->{islandviewer_home}) {
        my $ivhome = $cfg->{islandviewer_home};
        print QSUB "# Setting islandviewer home directory\n";
        #print QSUB "#PBS -V HOME=\"$ivhome\"\n\n";
        $env_var = "HOME=\"$ivhome\" ";
    }
    
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
        print QSUB "# Setting MicrobeDB database to use\n";
        #print QSUB "#PBS -V MicrobeDB= \"$microbedb_database\"\n\n";
        #print QSUB "export MicrobeDB=\"$microbedb_database\"\n\n";
        $env_var .= "MicrobeDB=\"$microbedb_database\" ";
    }
    
    if($cfg->{microbedb_config}) {
        my $microbedb_config = $cfg->{microbedb_config};
        print QSUB "# Setting microbedb config file with credentials to use\n";
        #print QSUB "#PBS -V MicrobeDB_config=\"$microbedb_config\"\n\n";
        $env_var .= "MicrobeDB_config=\"$microbedb_config\" ";
    }
    
    my $env_cmd = $env_var . $cmd; 
    print QSUB "echo \"Starting submission, command: $env_cmd\"\n";
    print QSUB "$env_cmd\n";

    close QSUB;
}

1;
