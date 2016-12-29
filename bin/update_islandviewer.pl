#!/usr/bin/env perl

use warnings;
use strict;
use Cwd qw(abs_path getcwd);
use Getopt::Long;
use Date::Manip;
use File::Spec::Functions;
use JSON;
use Data::Dumper;

BEGIN{
# Find absolute path of script
my ($path) = abs_path($0) =~ /^(.+)\//;
chdir($path);
sub mypath { return $path; }
};

use lib "../lib";
use lib "/home/lairdm/libs";
use Islandviewer;
use Islandviewer::Config;
use Islandviewer::DBISingleton;
use Islandviewer::Distance;
use Islandviewer::Genome_Picker;

use MicrobedbV2::Singleton;

my $host = 'localhost';
my $port = 8211;
my $handle;
my $alarm_timeout = 60;
my $protocol_version = '1.0';
my $microbedb_ver;
my $logger;

MAIN: {
    my $cfname; my $doislandpick; my $picker_obj;
    my $skip_distance; my $update_only; my $distance_only;
    my $num_jobs = 200;
    my $scheduler = 'Islandviewer::Torque';
    my $res = GetOptions("config=s" => \$cfname,
			 "do-islandpick" => \$doislandpick,
                         "skip-distance" => \$skip_distance,
                         "update-only" => \$update_only,
                         "distance-only" => \$distance_only,
                         "distance-jobs=s" => \$num_jobs,
                         "distance-scheduler=s" => \$scheduler,
    );

    die "Error, no config file given"
      unless($cfname);

    my $Islandviewer = Islandviewer->new({cfg_file => $cfname });

    my $cfg = Islandviewer::Config->config;

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
	$app->file_switch($cfg->{logdir} . "/ivupdate.$datestr.log");
    }

    my $base_work_dir = catdir($cfg->{workdir},"$datestr");
    $logger->debug("Making working directory for distance $base_work_dir");
    if( -d $base_work_dir) {
	$logger->logdie("Error, workdir already exists for today, not proceeding");
    }
    mkdir $base_work_dir;

    $logger->info("Connecting to Islandviewer server $host:$port");
    $host = $cfg->{daemon_host}
        if($cfg->{daemon_host});
    $port = $cfg->{tcp_port}
        if($cfg->{tcp_port});

    my $sets_run;
    my $sets_run_last_cycle = 99999999;
    my $cycle_num = 1;

    # We're going to loop until we stop computing more distances,
    # this will catch dying children that might cause some of our
    # distances to not be caught
    my $loop_inf = $skip_distance ? 0 : 1;
    while($loop_inf) {
	eval{
	    # We need the trailing slash becauce the code that uses this expects
	    # it, my bad...
	    my $cycle_workdir =  catdir($base_work_dir, "cycle$cycle_num") . '/';
	    $logger->debug("Making workdir for cycle $cycle_num: $cycle_workdir");
	    mkdir $cycle_workdir;

	    my $dist_obj = Islandviewer::Distance->new({scheduler => $scheduler, workdir => $cycle_workdir, num_jobs => $num_jobs, block => 1 });

	    ($microbedb_ver,$sets_run) = $dist_obj->calculate_all();
	};
	if($@) {
	    die "Error updating islandviewer in distance phase: $@";
	}

	if($sets_run == 0) {
	    $logger->info("No sets to run, moving on...");
	    last;
	} elsif($sets_run < $sets_run_last_cycle) {
	    $logger->info("We ran $sets_run this attempt, $sets_run_last_cycle last time");
	} elsif($sets_run == $sets_run_last_cycle) {
	    # This can either be if its stuck not getting more or if it hits zero
	    $logger->info("We ran the same number of sets as last cycle ($sets_run), moving on...");
	    last;
	} else {
	    $logger->logdie("Something really weird happened, this cycle: $sets_run, last cycle: $sets_run_last_cycle");
	}

	$sets_run_last_cycle = $sets_run;

        # And incremenet the cycle for the next iteration
        $cycle_num++;
    }

    if($distance_only) {
        $logger->info("Doing Distance only, exiting.");
        exit;
    }

    # We should have all the distances done now, let's do the IV
    my $microbedb = MicrobedbV2::Singleton->fetch_schema;

    unless($microbedb_ver) {
        $logger->warn("We don't have a microbedb version set, did we skip distance calculation?");

        # Oh, we skipped doing the distance, that's ok, just grab the latest version
        if($skip_distance) {
            $microbedb_ver = $microbedb->latest();
        } else {
            die "Error, this should never happen, we don't seem to have a valid microbedb version: $microbedb_ver";
        }
    }

    # Initializing backend connection and making picker obj is we'r edoing an Islandpick update too
    if($doislandpick) {
	$picker_obj = Islandviewer::Genome_Picker->new({microbedb_version => $microbedb_ver});

    }

    $logger->info("Finding all replicons in microbedb version $microbedb_ver");

    # Find all the replicons in this version
    my $rep_results = $microbedb->resultset('Replicon')->search( {
        rep_type => 'chromosome',
        version_id => $microbedb_ver
                                                              }
    );

    my $dbh = Islandviewer::DBISingleton->dbh;
    my $check_analysis = $dbh->prepare("SELECT aid, microbedb_ver FROM Analysis WHERE ext_id = ? and default_analysis = 1 ORDER BY microbedb_ver DESC");

    my $find_analysis = $dbh->prepare("SELECT Analysis.aid, GIAnalysisTask.parameters FROM Analysis, GIAnalysisTask WHERE Analysis.aid = ? AND Analysis.aid = GIAnalysisTask.aid_id AND prediction_method = 'Islandpick' AND default_analysis = 1");

    # We're going to use the same arguments for all the runs
    my $args->{Islandpick} = {
			      MIN_GI_SIZE => 4000};
    $args->{Sigi} = {
			      MIN_GI_SIZE => 4000};
    $args->{Dimob} = {
			      extended_ids => 1, MIN_GI_SIZE => 4000};
    $args->{Distance} = {block => 1, scheduler => 'Islandviewer::NullScheduler'};
    $args->{microbedb_ver} = $microbedb_ver;
    $args->{default_analysis} = 1;
    $args->{email} = 'lairdm@sfu.ca';

    my $count = 0;

    while( my $curr_rep = $rep_results->next() ) {
	my $accnum = $curr_rep->rep_accnum . '.' . $curr_rep->rep_version;
        $logger->debug("Testing if we should run $accnum");

	# Has this replicon already been run before?
	$check_analysis->execute($accnum);

	if(my @row = $check_analysis->fetchrow_array) {
	    $logger->info("We already have $accnum in the database as analysis $row[0]");

            # Skip this step of checking Islandpicks if we've been instructed to
            # and the existing Analysis isn't from this version of Microbedb we're
            # updating to
            unless($doislandpick && ($microbedb_ver != $row[1])) {
                $logger->trace("Skipping (doislandpick: $doislandpick), current microbedb $microbedb_ver vs previous run version " . $row[1]);
                next;
            }

	    $logger->debug("Checking if we should try rerunning Islandpick");
	    $find_analysis->execute($row[0]);

	    # See if there's an existing Islandpick for this analysis,
	    # if not, rerun it to see if we now get an Islandpick using the new
	    # genomes we've added
	    if(my @a_row = $find_analysis->fetchrow_array) {
		eval {
		    my $json_obj = from_json($a_row[1]);

                    # See if we'd have any comparison genomes for this replicon
                    my $picked_genomes = $picker_obj->find_comparative_genomes($accnum);
                    $logger->trace("Got back from picker: " . Dumper($picked_genomes));
                    my @comparison_genomes = ();

                    # Loop through the results
                    foreach my $tmp_rep (keys %{$picked_genomes}) {
                        # If it wasn't picked, we don't want it
                        next unless($picked_genomes->{$tmp_rep}->{picked});
                        
                        # Push it on the list of comparison genomes
                        push @comparison_genomes, $tmp_rep;
                    }

                    # None? Leave this replicon alone
                    unless(@comparison_genomes) {
                        $logger->info("No comparison genomes found for $accnum");
                        next;
                    }

                    @comparison_genomes = sort @comparison_genomes;
                    $logger->info("Found Islandpick comparison genomes: @comparison_genomes");

#                    $logger->trace("All candidate genomes: " . Dumper($picked_genomes));

                    # If we previously had comparison genomes last time this
                    # replicon was run, see if they've changed and we need to rerun
		    if ($json_obj->{comparison_genomes}) {
			my @old_comparison_genomes = split ' ', $json_obj->{comparison_genomes};
                        @old_comparison_genomes = sort @old_comparison_genomes;
			$logger->info("Existing Islandpick found: @old_comparison_genomes");

			
			$logger->info("Found comparison genomes: @comparison_genomes");

			unless(@comparison_genomes ~~ @old_comparison_genomes) {
			    $logger->info("Picked genomes don't match previous version, resubmitting Islandpick");
			    my $genomes_str = join ' ', sort(@comparison_genomes);
			    $logger->debug("Submitting new comparison genomes: $genomes_str");

			    my $message = build_req($row[0], $genomes_str);
			    my $received = send_req($message);
			    $logger->info("Received back message: " . $received);
			}
			
		    } else {
                        # We didn't have comparison genomes last time, but we should
                        # only get here if we have them now

			my $message = build_req($row[0]);
			$logger->info("No existing Islandpick found for analysis, rerunning " . $row[0]);
			my $received = send_req($message);
			$logger->info("Received back message: " . $received);
		    }
		};
		if($@) {
		    $logger->error("Error decoding json or submitting ". $row[0] . ": " . $row[1] . ": " . $@);
		}
	    }
	    
	    # Move on to the next replicon
	    next;
	} else {
            if($update_only) {
                $logger->debug("We've been told to only update, not submit new, skipping $accnum");
                next;
            }

	    # Else its new so add it to the name cache
	    $dbh->do("INSERT IGNORE INTO NameCache (cid, name, rep_size, cds_num) VALUES (?, ?, ?, ?)", undef,
		     $accnum,
		     $curr_rep->definition,
		     $curr_rep->rep_size,
		     $curr_rep->cds_num
		) or $logger->logdie("Error inserting in to NameCache for $accnum, " . $curr_rep->definition);
	}
	
	# Submit the replicon for processing
	my $aid = 0;
	my $starttime = time;
	eval {
	    $aid = $Islandviewer->submit_analysis($accnum, $args);
	};
	if($@) {
	    $logger->error("Error submitting analysis $accnum: $@");
	}
	if($aid) {
	    $logger->debug("Finished submitting $accnum, has analysis id $aid");
	} else {
	    $logger->error("Error submitting $accnum, didn't get an aid");
	}
	if($count >= 250) {
	    $logger->info("250 submitted, sleeping for 15 minutes");
	    sleep 900;
	    $count = 0;
	    next;
#	    last;
	}
	$count++;
	my $diff = time - $starttime;
	# We don't want to submit too quickly....
	if($diff < 15) {
	    $logger->trace("Submission ran too quickly, pausing " . (15 - $diff) . ' seconds');
	    sleep abs(15 - $diff);
	}

    }

    $logger->info("All analysis should now be submitted");
}

sub myconnect {
    my $host = shift;
    my $port = shift || 8211;

    $handle = IO::Socket::INET->new(Proto     => "tcp",
				    PeerAddr  => $host,
				    PeerPort  => $port)
       or die "can't connect to port $port on $host: $!";

}

sub disconnect {

    # Closing connection
    $logger->trace("Closing connection to backend");
    $handle->shutdown(2);
    $handle->close();

    $handle = undef;
}

sub send_req {
    my $msg = shift;
    my $received = '';

    $logger->trace("Sending: $msg\n");

    $logger->trace("Connecting to backend");
    myconnect($host, $port);

    # Check if the message ends with a LF, if not
    # add one
    $msg .= "\n" unless($msg =~ /\n$/);
    my $length = length $msg;

    # Make sure the socket is still open and working
    if(! defined $handle->connected) {
#    if($handle->connected ~~ undef) {
	return;
    }

    # Set up an alarm, we don't want to get stuck
    # since we are allowing blocking in the send
    # (the server might not be ready to receive, it's
    # not multi-threaded, just multiplexed)
    eval {
	local $SIG{ALRM} = sub { die "timeout\n" };
	alarm $alarm_timeout;
	
	# While we still have data to send
	while($length > 0) {
	    my $rv = $handle->send($msg, 0);

	    # Oops, did we fail to send to the socket?
	    unless(defined $rv) {
		# Turn the alarm off!
		alarm 0;
		return undef;
	    }

	    # We've sent some or all of the buffer, record that
	    $length -= $rv;

	}

	# The message is sent, now we wait for a reply, or until
	# our alarm goes off
	while($received !~ /\n$/) {
	    my $data;
	    # Receive the response and put it in the queue
	    my $rv = $handle->recv($data, POSIX::BUFSIZ, 0);
	    unless(defined($rv)) {
		alarm 0;
		return undef;
	    }
	    $received .= $data;

	    if($received) {
		my $status = $handle->send(' ', 0);
		unless(defined($status)) {
		    last;
		}
	    }
	}

	# We've successfully made our request, clear the alarm
	alarm 0;
    };
    # Did we get any errors back?
    if($@) {

        $logger->error("Error communicating with backend: $@");

        disconnect();

	# Uh-oh, we had an alarm, the iteration timed out
	if($@ eq "timeout\n") {
	    return undef;
	} else {
	    return undef;
	}
    }

    disconnect();

    # Success! Return the results
    return $received;
}

sub build_req {
    my $aid = shift;
    my $comparison_genomes = @_ ? shift : undef;

    my $compare_hash = ( $comparison_genomes ? { comparison_genomes => $comparison_genomes } : { } );

    my $modules = { Islandpick => { args => $compare_hash },
                    distance => { args => { reset => 1 } } };

    my $req = { version => $protocol_version,
		action => 'clone',
		aid => $aid,
		args => { modules => $modules,
                          owner_id => 0,
                          default_analysis => 1,
                          demote_old => 1,
                          microbedb_ver => $microbedb_ver }
    };

    my $req_str = to_json($req, { pretty => 1});

    $req_str .= "\nEOF";

    my $str = "{\n \"version\": \"$protocol_version\",\n";
    $str .= " \"action\": \"clone\",\n";
#    $str .= " \"max_cutoff\": \"0.48\",\n";
#    $str .= " \"max_compare_cutoff\": \"10\",\n";
    $str .= " \"aid\": \"$aid\"\n";
    $str .= " }\nEOF";

    return $req_str;
}
