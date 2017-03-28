=head1 NAME

    Islandviewer::Notification

=head1 DESCRIPTION

    Maintain and do notifications on analysis

=head1 SYNOPSIS

    use Islandviewer::Notification;



=head1 AUTHOR

    Matthew Laird
    Brinkman Laboratory
    Simon Fraser University
    Email: lairdm@sfu.ca

=head1 CREATED

    Dec 15, 2014

=cut

package Islandviewer::Notification;

use strict;
use Moose;
use Log::Log4perl qw(get_logger :nowarn);
use JSON;
use Email::Valid;
use Mail::Mailer;
use Data::Dumper;

use Islandviewer::DBISingleton;
use Islandviewer::Constants qw(:DEFAULT $STATUS_MAP $REV_STATUS_MAP $ATYPE_MAP);

has 'notifications' => (
    traits  => ['Hash'],
    is      => 'rw',
    isa     => 'HashRef',
    default => sub { {} },
    handles => {
         set_notification    => 'set',
         get_notification    => 'get',
         delete_notification => 'delete',
         clear_notification  => 'clear',
         fetch_keys          => 'keys',
         fetch_values        => 'values',
         notification_pairs  => 'kv',
    },
);

my $cfg; my $logger;

sub BUILD {
    my $self = shift;
    my $args = shift;

    $cfg = Islandviewer::Config->config;

    $logger = Log::Log4perl->get_logger;

    if($args->{aid}) {
	$self->{aid} = $args->{aid};
	$self->load_notifications();
    } else {
	$logger->logdie("Error, you must associate a Notification object with an analysis");
    }
}

sub load_notifications {
    my $self = shift;

    my $dbh = Islandviewer::DBISingleton->dbh;
    my $fetch_notifications = $dbh->prepare("SELECT email, status FROM Notification WHERE analysis_id = ?") or $logger->logdie("Error, can't prepare fetch notifications: $DBI::errstr");

    $fetch_notifications->execute($self->{aid}) or $logger->logdie("Error, can't fetch notifications: $DBI::errstr");
    while(my @row = $fetch_notifications->fetchrow_array) {
	$self->set_notification($row[0] => $row[1]);
    }
    
}

sub add_notification {
    my $self = shift;
    my $email = shift;

    unless( Email::Valid->address($email) ) {
	$logger->logdie("Invalid email address: $email");
    }

    my $dbh = Islandviewer::DBISingleton->dbh;

    my $add_notification = $dbh->prepare("INSERT IGNORE INTO Notification (analysis_id, email, status) VALUES (?, ?, ?)") or  $logger->logdie("Error, can't prepare set notifications: $DBI::errstr");

    $add_notification->execute($self->{aid}, $email, 0);

    $self->set_notification($email => 0);
}

sub notify {
    my $self = shift;
    my $status = shift;
    my $resend = shift;

    $logger->info("Sending notification emails for aid " . $self->{aid} . " status: $status, force resend: $resend");

    for my $email_pair ($self->notification_pairs()) {
	if($email_pair->[1] == 0 || $resend) {
	    $logger->trace("Sending email to " . $email_pair->[0]);
	    my $status = $self->send_email($email_pair->[0], $status);

	    if($status) {
		$self->set_sent($email_pair->[0]);
	    }
	}
    }
}

sub set_sent {
    my $self = shift;
    my $email = shift;

    my $dbh = Islandviewer::DBISingleton->dbh;

    eval {
	$dbh->do("UPDATE Notification SET status=1 WHERE analysis_id = ? AND email = ?", {}, $self->{aid}, $email)
	    or $logger->logdie("Error, can't updae status for " . $self->{aid} . ", email $email: $DBI::errstr");
    };
    if($@) {
	return 0;
    }

    $self->set_notification($email => 1);
    return 1;
}

sub send_email {
    my $self = shift;
    my $email = shift;
    my $status = shift;

    unless($status == $STATUS_MAP->{COMPLETE} || $status == $STATUS_MAP->{ERROR}) {
	$logger->warn("We don't send emails out for jobs not in complete or error states, this job was in: " . $REV_STATUS_MAP->{$status} . " ($status)");
	return 0;
    }

    my $mailer = Mail::Mailer->new();

    my $url = $cfg->{base_url} . 'results/';

    my $token = $self->fetch_token();
    $url .= "$token" if $token;

    eval {
	$mailer->open({ From    => $cfg->{email_sender},
			To      => $email,
			Subject => 'Islandviewer Results'
		      })
	    or $logger->logdie("Error, can't open email: $!");
    
	print $mailer "IslandViewer Notice\n\n";
	$logger->trace("Sending notification for " . $self->{aid} . ", status: " . $status);
	if($status == $STATUS_MAP->{COMPLETE}) {
	    print $mailer "Genomic islands have finished being predicted using IslandPick, SIGI-HMM, and IslandPath-DIMOB!\n\n";
	    print $mailer "Please use the following link to continue using IslandViewer:\n";
	} elsif($status == $STATUS_MAP->{ERROR}) {
	    print $mailer "We're sorry, there seems to be an issue running your Islandviewer job, for more information please see the url below or contact us for more assistance.\n\n";
	}

	print $mailer "$url\n";

	$mailer->close();
    };
    if($@) {
	$logger->error("Error sending email to $email: $@");
	return 0;
    }

    return 1;
}

sub fetch_token {
    my $self = shift;

    my $dbh = Islandviewer::DBISingleton->dbh;

    my $fetch_analysis = $dbh->prepare("SELECT token from Analysis WHERE aid = ?");
    $fetch_analysis->execute($self->{aid});

    if(my @row = $fetch_analysis->fetchrow_array) {
        return $row[0];
    }

    return undef;
}

1;
