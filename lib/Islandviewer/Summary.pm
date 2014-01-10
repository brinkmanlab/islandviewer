=head1 NAME

    Islandviewer::Summary

=head1 DESCRIPTION

    Object to run the final summary/cleanup of an Islandviewer job

=head1 SYNOPSIS

    use Islandviewer::Summary;

    $vir_obj = Islandviewer::Summary->new({workdir => '/tmp/workdir',
                                         microbedb_version => 80});

=head1 AUTHOR

    Matthew Laird
    Brinkman Laboratory
    Simon Fraser University
    Email: lairdm@sfu.ca

=head1 LAST MAINTAINED

    Dec 21, 2013

=cut

package Islandviewer::Summary;

use strict;
use Moose;
use Log::Log4perl qw(get_logger :nowarn);
use File::Temp qw/ :mktemp /;
use Data::Dumper;

use Islandviewer::DBISingleton;
use Islandviewer::IslandFetcher;

my $cfg; my $logger; my $cfg_file;

my $module_name = 'Summary';

sub BUILD {
    my $self = shift;
    my $args = shift;

    $cfg = Islandviewer::Config->config;
    $cfg_file = File::Spec->rel2abs(Islandviewer::Config->config_file);

    $logger = Log::Log4perl->get_logger;

    die "Error, work dir not specified:  $args->{workdir}"
	unless( -d $args->{workdir} );
    $self->{workdir} = $args->{workdir};

    
}

sub run {
    my $self = shift;
    my $accnum = shift;
    my $callback = shift;

    my $status_set = $callback->fetch_module_statuses();

    for my $mod (keys %{$status_set}) {
	# Don't check ourself
	next if($mod eq $module_name);

	if($status_set->{$mod}->{status} ne 'COMPLETE') {
	    # We *may* have a problem....

	    if($status_set->{$mod}->{required}) {
		# We're required to be successful and we're not.
		# This catches non-run modules, which if they've
		# not run by this point are a failure.
		$callback->set_status('ERROR');
		return 0;
	    } elsif($status_set->{$mod}->{status} ne 'ERROR') {
		# We're not required to be successful, but we're
		# not in error either, didn't run? That's a problem.
		$callback->set_status('ERROR');
		return 0;
	    }

	    # Ok, no problem after all, we were in error, but we're
	    # not required to be successful.
	}
    }
    # Otherwise if any other module failed, that's ok, we're going
    # to declare victory.

    # We actually do nothing with this right now except set the analysis
    # as complete, later we should do a check through of the modules
    # to ensure they all ran correctly and maybe notify of problems
    $callback->set_status('COMPLETE');

    return 1;
}
