=head1 NAME

    Islandviewer

=head1 DESCRIPTION

    Object for managing islandviewer jobs

=head1 SYNOPSIS

    use Islandviewer;


=head1 AUTHOR

    Matthew Laird
    Brinkman Laboratory
    Simon Fraser University
    Email: lairdm@sfu.ca

=head1 LAST MAINTAINED

    Sept 25, 2013

=cut

package Islandviewer;

use strict;
use Moose;
use Data::Dumper;
use Date::Manip;
use File::Spec;

use Islandviewer::Config;
use Islandviewer::DBISingleton;
use Islandviewer::GenomeUtils;
use Islandviewer::Analysis;
use Islandviewer::Notification;

use Net::ZooKeeper::WatchdogQueue;

use MicrobedbV2::Singleton;

use Log::Log4perl qw(get_logger :nowarn);

my $cfg; my $logger;

sub BUILD {
    my $self = shift;
    my $args = shift;

    # Initialize the configuration file
    Islandviewer::Config->initialize({cfg_file => $args->{cfg_file} });
    $cfg = Islandviewer::Config->config;

    # Initialize the DB connection
    Islandviewer::DBISingleton->initialize({dsn => $cfg->{'dsn'}, 
                                             user => $cfg->{'dbuser'}, 
                                             pass => $cfg->{'dbpass'} });
   
    $logger = Log::Log4perl->get_logger;

    # Initialize the logfile with the current date
    $self->log_rotate();

    $logger->trace("Islandviewer initialized");
    $logger->trace("Process umask is: " . umask);

}

# Submit a file, it will be a custom genome,
# load it in to the database and ensure
# all needed formats are there

sub submit_and_prep {
    my $self = shift;
    my $file = shift;
    my $genome_name = (@_ ? shift : 'custom_genome');

    $logger->info("Received file $file, checking and loading");
    unless(-f $file && -s $file) {
	$logger->logdie("Error, can't access $file");
    }

    # Make out genomeutils object
    my $genome_obj = Islandviewer::GenomeUtils->new();

    # Load it and ensure we have all the proper formats
    $genome_obj->read_and_convert($file, $genome_name);

    # Sanity checking, did we get all the correct types?
    unless($cfg->{expected_exts} eq $genome_obj->find_file_types()) {
	$logger->error("Error, we don't have the correct file types, we can't continue, have \"" . $genome_obj->find_file_types() . "\", expecting \"$cfg->{expected_exts}\"");
	return 0;
    }

    # We put it in the database
    my $cid = $genome_obj->insert_custom_genome();

    # Now we need to move things in to place, so we're nice
    # and tidy with our file organization
    my $cid_dir = File::Spec->catpath(undef, $cfg->{custom_genomes}, "$cid");
    unless(mkdir($cid_dir)) {
	$logger->error("Error, can't make custom genome directory $cid_dir: $!");
	return 0;
    }
    unless($genome_obj->move_and_update($cid, $cid_dir)) {
	$logger->error("Error, can't move files to custom directory $cid_dir for cid $cid");
    }

    # We're ready to go... return our new cid
    return $cid;
}

sub log_rotate {
    my $self = shift;

    # Build the logfile name we want to use
    my $datestr = UnixDate("now", "%Y%m%d");
    my $logfile = File::Spec->catpath(undef, $cfg->{logdir}, "islandviewer.$datestr.log");

    my $app = Log::Log4perl->appender_by_name("errorlog");

    unless($app) {
	$logger->warn("Logging doesn't seem to be defined, you might not even see this");
	return;
    }

    if($app->filename eq $logfile) {
	$logger->info("Asked to rotate the logfile, nothing to do, keeping " . $app->filename);
    } else {
	$logger->info("Rotating log file, current file: " . $app->filename . ", switching to: " . $logfile);
	$app->file_switch($logfile);
	$logger->info("Initializing logfile, rotated.");
    }
}

# We're trying to keep things abstract as possible
# but at some level someone has to know about all
# the pipeline components, its not this guy.

sub submit_analysis {
    my $self = shift;
    my $cid = shift;
    my $args = shift;

    # If we've been given a microbedb version AND its valid...
    # Yes we do this in Analysis too, but I didn't think through we need
    # the version when looking up microbedb genomes in a GenomeUtil object
    # Create a Versions object to look up the correct version
    my $microbedb = MicrobedbV2::Singleton->fetch_schema;

    my $microbedb_ver;
    if($args->{microbedb_ver} && $microbedb->fetch_version($args->{microbedb_ver})) {
	$microbedb_ver = $args->{microbedb_ver}
    } else {
	$microbedb_ver = $microbedb->current();
    }

    my $genome_utils = Islandviewer::GenomeUtils->new({microbedb_ver => $microbedb_ver });

    my $genome_obj = $genome_utils->fetch_genome($cid);

#    my($name, $base_filename, $types) = $genome_obj->lookup_genome($cid);

    my $name = $genome_obj->name();
    my $base_filename = $genome_obj->filename();

    $logger->trace("For cid $cid we found filenames $name, $base_filename, " . Dumper($genome_obj->formats()));
    unless($base_filename) {
	$logger->error("Error, we couldn't find cid $cid ($base_filename)");
	return 0;
    }

    # Sanity checking, did we get all the correct types?
    # Since we moved file generation to the Prepare module,
    # we want at least a genbank of embl file

    if('.gbk' ~~ $genome_obj->formats()) {
	$logger->trace("We have a genbank file (prefered), we're ok to submit");
    } elsif('.embl' ~~ $genome_obj->formats()) {
	$logger->trace("We have a embl file, we're ok to submit");
    } else {
	$logger->error("We don't have all the file types we need, only have: " . Dumper($genome_obj->formats()) );
	return 0;
    }

#    unless($cfg->{expected_exts} eq $genome_obj->find_file_types()) {
	# We need to regenerate the files
#	$logger->trace("We don't have all the file types we need, only have: " . $genome_obj->find_file_types());
#	unless($genome_obj->regenerate_files()) {
	    # Oops, we weren't able to regenerate for some reason, failed
#	    $logger->error("Error, we don't have the needed files, we can't do an alaysis ($base_filename)");
#	    return 0;
#	}
#    }    

    # Ensure we have our GC values calculated and ready to go
    # Moved to Prepare module
#    $genome_obj->insert_gc($cid);

    # We should be ready to go, let's submit our analysis!
    my $analysis_obj = Islandviewer::Analysis->new({workdir => $cfg->{analysis_directory}});
    my $aid;

    eval {
	$aid = $analysis_obj->submit($genome_obj, $args);

	if($aid && $args->{email}) {
	    $self->add_notification($aid, $args->{email});
	}
    };
    if($@) { 
	$logger->error("Error, we couldn't submit the analysis: $@");
	return 0;
    }

    return $aid;
}

# Clone an analysis and rerun the requested
# modules with the updated defaults

sub rerun_job {
    my $self = shift;
    my $aid = shift;
    my $args = shift;
    my $new_aid = 0;

    $logger->info("Cloning and restarting analysis $aid");
    my $analysis_obj = Islandviewer::Analysis->new({workdir => $cfg->{analysis_directory}, aid => $aid});

    eval {
	my $new_analysis_obj;

	# If we're told to clone, do so, otherwise just use
	# the existing analysis object
	if($args->{clone}) {
            my $clone_args = {};
            # Pass in the owner of the cloned job if given one
            if(exists $args->{owner_id}) {
                $logger->trace("New owner_id for $aid: " . $args->{owner_id});
                $clone_args->{owner_id} = $args->{owner_id};
            }

            # Pass in the owner of the cloned job if given one
            if($args->{default_analysis}) {
                $logger->trace("New default_analysis for $aid: " . $args->{default_analysis});
                $clone_args->{default_analysis} = $args->{default_analysis};
            }

            # If we have a new microbedb version for our cloned analysis
            # pass that in
            if($args->{microbedb_ver}) {
                $logger->trace("New microbedb_ver for $aid: " . $args->{microbedb_ver});
                $clone_args->{microbedb_ver} = $args->{microbedb_ver};
            }

            # If were told to demote the old analysis
            if($args->{demote_old}) {
                $logger->trace("Demoting old analysis $aid: " . $args->{demote_old});
                $clone_args->{demote_old} = $args->{demote_old};
            }

            $new_analysis_obj = $analysis_obj->clone($clone_args);

	} else {
	    $new_analysis_obj = $analysis_obj;
	}

	my @modules;
	foreach my $m (keys $args->{modules}) {
	    $logger->trace("Purging module $m for cloned analysis $aid, new aid: $new_analysis_obj->{aid}");
	    push @modules, $m;
	    # If we're actually changing arguments, rather than just rerunning 
	    # a module, set those arguments
	    $new_analysis_obj->fetch_module($m);

	    if($args->{modules}->{$m}->{args}) {
		my $old_args = $new_analysis_obj->{module_args};

                # Were we told to completely reset the module and
                # run it again with whatever defaults it decides
                # during it's normal run?
                if($args->{modules}->{$m}->{args}->{reset}) {
                    $old_args = {};
                } else {
                    # Else we might have been arguments to update (maybe not,
                    # maybe we're just rerunning with the existing arguments
                    # from the previous run)
                    
                    # Loop through and update the arguments with the new
                    # values we've received
                    foreach my $a (keys $args->{modules}->{$m}->{args}) {
                        $old_args->{$a} = $args->{modules}->{$m}->{args}->{$a};
                    }
                }

                # We're updated or reset the arguments,
                # save those updates
		$new_analysis_obj->update_args($old_args, $m);
	    }
	    $new_analysis_obj->purge($m);
	    $new_analysis_obj->set_module_status('PENDING');
	}

	if(@modules) {
	    # If we're rerunning any modules we need to rerun
	    # Virulence to update the gene-gi mappings
	     $new_analysis_obj->fetch_module('Virulence');
	     my $old_args = $new_analysis_obj->{module_args};
	     $old_args->{modules} = \@modules;
	     $logger->trace("Resetting Virulence module for aid $new_analysis_obj->{aid} with new arguments: " . Dumper($old_args));
	     $new_analysis_obj->update_args($old_args, 'Virulence');
	     $new_analysis_obj->set_module_status('PENDING');
	}

	# And we always need to reset Summary, to get things to finish
	# nicely
	$logger->trace("Resetting Summary module to Pending for aid $self->{aid}");
	$new_analysis_obj->fetch_module('Summary');
	$new_analysis_obj->set_module_status('PENDING');

	$new_analysis_obj->set_status('PENDING');

	# Need something here to deal with email addresses
	$new_analysis_obj->submit_to_scheduler({});

	$logger->trace("Finished cloning analysis, new aid $new_analysis_obj->{aid}");
	$new_aid = $new_analysis_obj->{aid};

    };
    if($@) {
	$logger->error("Error cloning and restarting analysis $aid: $@");
	return 0;
    }

    return $new_aid;

}

sub generate_token {
    my $self = shift;
    my $aid = shift;

    $logger->trace("Generating security token for aid $aid");
    my $analysis_obj = Islandviewer::Analysis->new({workdir => $cfg->{analysis_directory}, aid => $aid});

    my $token = $analysis_obj->generate_token();

    return $token;
}

sub add_notification {
    my $self = shift;
    my $aid = shift;
    my $email = shift;

    $logger->trace("Adding email notification: " . $email);

    eval {
	my $notification = Islandviewer::Notification->new({aid => $aid});
	$notification->add_notification($email);
    };
    if($@) {
	$logger->logdie("Error, failure to set email notification: $@");
    }

    return 1;
}

sub run {
    my $self = shift;
    my $aid = shift;
    my $module = shift;

    # Create our watchdog so observers can keep an eye
    # on our status
    my $watchdog = new Net::ZooKeeper::WatchdogQueue($cfg->{zookeeper},
						     $cfg->{zk_analysis});

    my $analysis_obj = Islandviewer::Analysis->new({workdir => $cfg->{analysis_directory}, aid => $aid});

    my $ret;
    eval {
	$watchdog->create_timer("$aid.$module");
	$ret = $analysis_obj->run($module);
    };
    if($@) {
	$logger->error("Error running module $module: $@");
	return 0;
    }

    return $ret;
}

1;
