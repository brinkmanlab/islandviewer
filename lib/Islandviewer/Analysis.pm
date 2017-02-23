=head1 NAME

    Islandviewer::Analysis

=head1 DESCRIPTION

    Object to maintain and evaluate an analysis

=head1 SYNOPSIS

    use Islandviewer::Analysis;



=head1 AUTHOR

    Matthew Laird
    Brinkman Laboratory
    Simon Fraser University
    Email: lairdm@sfu.ca

=head1 LAST MAINTAINED

    Nov 15, 2013

=cut

package Islandviewer::Analysis;

use strict;
use Moose;
use Log::Log4perl qw(get_logger :nowarn);
use JSON;
use Data::Dumper;
use File::Copy::Recursive qw(dircopy);
use Session::Token;
use File::Spec;

use Islandviewer::DBISingleton;
use Islandviewer::Constants qw(:DEFAULT $STATUS_MAP $REV_STATUS_MAP $ATYPE_MAP);
use Islandviewer::Notification;

use MicrobedbV2::Singleton;

my $cfg; my $logger; my $cfg_file;

my @modules = qw(Prepare Distance Islandpick Sigi Dimob Virulence Summary);
my @secondary_modules = qw(ContigAligner);
my @required_success = qw(Prepare Distance Virulence);

sub BUILD {
    my $self = shift;
    my $args = shift;

    $cfg = Islandviewer::Config->config;
    $cfg_file = File::Spec->rel2abs(Islandviewer::Config->config_file);

    $logger = Log::Log4perl->get_logger;

    die "Error, work dir not specified:  $args->{workdir}"
	unless( -d $args->{workdir} );
    $self->{base_workdir} = $args->{workdir};

    # We're loading an existing analysis
    if($args->{aid}) {
	$self->{aid} = $args->{aid};
	$self->load_analysis($args->{aid});
    }

}

sub change_logfile {
    my $self = shift;

    if($cfg->{daemon}) {
	$logger->trace("We're in daemon mode, not changing the log file");
	return;
    }

    my $app = Log::Log4perl->appender_by_name("errorlog");
    if($self->{workdir}) {
	$logger->trace("Switching log to: " . $self->{workdir} . "/analysis.log");
	$app->file_switch($self->{workdir} . "/analysis.log");

    } else {
	$logger->trace("Switching log to: " . $self->{base_workdir} . "/analysis.log");
	$app->file_switch($self->{base_workdir} . "/analysis.log");
    }
    $logger->debug("Logging initialized, aid $self->{aid}");
}

sub load_analysis {
    my $self = shift;
    my $aid = shift;

    my $dbh = Islandviewer::DBISingleton->dbh;
	
    my $fetch_analysis = $dbh->prepare("SELECT atype, ext_id, default_analysis, status, workdir, microbedb_ver, token FROM Analysis WHERE aid = ?");

    $fetch_analysis->execute($aid) 
	or $logger->logdie("Error, can't fetch analysis $aid");
    
    # There should only be one
    if(my($atype, $ext_id, $default_analysis, $status, $workdir, $microbedb_ver, $token) =
       $fetch_analysis->fetchrow_array) {
	$self->{atype} = $atype;
	$self->{ext_id} = $ext_id;
	$self->{default_analysis} = $default_analysis;
	$self->{status} = $status;
	if($workdir =~ /{{.+}}/) {
	    $workdir =~ s/{{([\w_]+)}}/$cfg->{$1}/eg;
	}
	$self->{base_workdir} = $workdir;
	$self->{microbedb_ver} = $microbedb_ver;
        $self->{token} = $token if $token;
    } else {
	$logger->logdie("Error, can't find analysis $aid");
    }

    unless( -d $self->{base_workdir} ) {
	$logger->logdie("Error, workdir " . $self->{base_workdir} . " doesn't exist for aid " . $self->{aid});
    }

    # Move the logging over to the analysis
    $self->change_logfile();

}

# Submit the analysis and all its pieces in to the
# database, this is the guy who has to know about all
# the various modules for a submission.

sub submit {
    my $self = shift;
    my $genome_obj = shift;
    my $args = shift;

    my $dbh = Islandviewer::DBISingleton->dbh;
    
    my $microbedb_ver;
    # Create a Versions object to look up the correct version
    my $microbedb = MicrobedbV2::Singleton->fetch_schema;

    # If we've been given a microbedb version AND its valid...
    if($args->{microbedb_ver} && $microbedb->fetch_version($args->{microbedb_ver})) {
	$microbedb_ver = $args->{microbedb_ver}
    } else {
	$microbedb_ver = $microbedb->latest();
    }

    # Submit the analysis!
    my $insert_analysis = $dbh->prepare("INSERT INTO Analysis (atype, ext_id, default_analysis, owner_id, status, microbedb_ver) VALUES (?, ?, ?, ?, ?, ?)");
    
    $logger->trace("Submitting analysis type: " . $genome_obj->atype() . ", id: " . $genome_obj->cid());
    $insert_analysis->execute($genome_obj->atype(), 
			      $genome_obj->cid(), 
			      ($args->{default_analysis} ? 1 : 0),
			      ($args->{owner_id} ? $args->{owner_id} : 0),
			      $STATUS_MAP->{PENDING},
			      $microbedb_ver
	) or $logger->logdie("Error inserting analysis, accnum $genome_obj->cid(): $DBI::errstr");

    # Now we fetch the analysis id, because this is needed in making
    # the workdir....
    my $aid = $dbh->last_insert_id(undef, undef, undef, undef);
    $self->{aid} = $aid;

    if($args->{email}) {
	$logger->trace("Adding email notification: " . $args->{email});

	eval {
	    my $notification = Islandviewer::Notification->new({aid => $self->{aid}});
	    $notification->add_notification($args->{email});
	};
	if($@) {
	    $self->set_status('ERROR');
	    $logger->error("Error, failure to set email notification: $@");
	    return 0;
	}
    }

    # We could do this with triggers but we won't, see below.
    # Make the workdir for our analysis
    $self->{workdir} = $self->{base_workdir} . "/$aid";
    unless(mkdir $self->{workdir}) {
	# Oops, we weren't able to make the workdir...
	$logger->error("Oops, we weren't able to make the workdir $self->{workdir} for analysis $aid");
	$self->set_status('ERROR');
	return 0;
    }

    # Move the logging over to the analysis
    # Let's not, this screws up mass submitting, save the log
    # file there for *running* the analysis
#    $self->change_logfile();

    # See if we can shorten the directory to allow moving
    # of install base in the future
    #
    # TODO: Move these calls to the Config module's universal
    #       path shortener
    my $shortened_workdir = $self->{workdir};
    if($cfg->{analysis_directory} && 
       $shortened_workdir =~ /$cfg->{analysis_directory}/) {
	$shortened_workdir =~ s/$cfg->{analysis_directory}/{{analysis_directory}}/;
	$logger->trace("Shortening workdir with analysis_directory: $shortened_workdir");
    } elsif($cfg->{workdir} &&
	    $shortened_workdir =~ /$cfg->{workdir}/) {
	$shortened_workdir =~ s/$cfg->{workdir}/{{workdir}}/;
	$logger->trace("Shortening workdir with workdir: $shortened_workdir");
    } elsif($cfg->{rootdir} && 
	    $shortened_workdir =~ /$cfg->{rootdir}/) {
	$shortened_workdir =~ s/$cfg->{rootdir}/{{rootdir}}/;
	$logger->trace("Shortening rootdir with workdir: $shortened_workdir");
    }

    $dbh->do("UPDATE Analysis SET workdir = ? WHERE aid = ?", undef, $shortened_workdir, $aid);

    # Alright, we have our analysis inserted, now time to add the components
    foreach my $mod (@modules) {
	$logger->trace("Adding GI task $mod");
	eval {
	    $self->submit_module($mod, $genome_obj, $args->{$mod});
	};
	if($@) {
	    $logger->error("Error adding $mod, $@");
	    return 0;
	}
    }

    # We probably need a better way to do this in the future
    #
    # TODO: Find a way to add secondary modules more efficiently
    foreach my $mod (@secondary_modules) {
	$logger->trace("Checking if we need to add secondary module $mod");
	eval {
	    if($args->{$mod}) {
		$logger->info("We have an entry in our config structure for $mod, adding module");
		$self->submit_module($mod, $genome_obj, $args->{$mod});
	    }
	};
	if($@) {
	    $logger->error("Error adding secondary module $mod, $@");
	    return 0;
	}
    }

    my $ret = $self->submit_to_scheduler($args);

    $logger->trace("Finished submitting aid $aid");
    return ($ret ? $aid : 0);
}

sub submit_to_scheduler {
    my $self = shift;
    my $args = shift;

    my $aid = $self->{aid};

    unless($self->{workdir}) {
	$logger->trace("We didn't find a workdir when submitting $self->{aid} to the schedule, using $self->{base_workdir}");
	$self->{workdir} = $self->{base_workdir};
    }

    # We need to submit the job to the scheduler
    $logger->debug("Submitting aid $aid to scheduler " . $cfg->{default_scheduler});
    my $scheduler;
    eval {
	no strict 'refs';
	$logger->trace("Initializing scheduler... (aid $aid)");
	(my $mod = "$cfg->{default_scheduler}.pm") =~ s|::|/|g; # Foo::Bar::Baz => Foo/Bar/Baz.pm
	require "$mod";
	$scheduler = "$cfg->{default_scheduler}"->new();
    };
    if($@) {
	$self->set_status('ERROR');
	$logger->logdie("Error, can't load scheduler (aid $aid): $@");
    }

    # We have a scheduler object, submit!
    my $job_type = ($cfg->{job_type} ? $cfg->{job_type} : 'Islandviewer');
    $job_type = $args->{job_type} if($args->{job_type});
    eval {
	$scheduler->build_and_submit($aid, $job_type, $self->{workdir}, $args, @modules);
    };
    if($@) {
	$logger->error("Error submitting analysis: $@");
	$self->set_status('ERROR');
	return 0;
    }

    $logger->trace("Finished submitting to scheduler aid $aid");
    return $aid;
}

sub submit_module {
    my $self = shift;
    my $module = shift;
    my $genome_obj = shift;
    my $args = shift;

    my $dbh = Islandviewer::DBISingleton->dbh;
    
    my $JSON_args;
    if($args) {
	$JSON_args = to_json($args);
    }

    $logger->trace("Adding module $module, json: $JSON_args");

    if($args->{skip}) {
	$logger->warn("We found a signal to skip module $module, returning...");
	return;
    }

    $dbh->do("INSERT INTO GIAnalysisTask (aid_id, prediction_method, status, parameters) VALUES (?, ?, ?, ?)", undef, 
	     $self->{aid},
	     $module,
	     $STATUS_MAP->{PENDING},
	     $JSON_args
	) or $logger->logdie("Error inserting analysis module $module: $DBI::errstr");

}

sub fetch_taskid {
    my $self = shift;
    my $module = shift;

    # Load the module information
    my $dbh = Islandviewer::DBISingleton->dbh;

    $logger->trace("Fetching taskid for module $module in analysis $self->{aid}");

    my $fetch_task = $dbh->prepare("SELECT taskid FROM GIAnalysisTask WHERE aid_id = ? AND prediction_method = ?");

    $fetch_task->execute($self->{aid}, $module) 
	or $logger->logdie("Error, can't fetch analysis $module");

    # There should only be one (highlander style)
    my($taskid);
    if(($taskid) =
       $fetch_task->fetchrow_array) {
	$logger->trace("Found taskid $taskid for module $module");
	return $taskid;

    } else {
	$logger->logdie("Error, can't find analysis task $module");
    }
    
}

sub fetch_module {
    my $self = shift;
    my $module = shift;

    # Load the module information
    my $dbh = Islandviewer::DBISingleton->dbh;

    $logger->trace("Fetching module $module in analysis $self->{aid}");

    my $fetch_task = $dbh->prepare("SELECT taskid, status, parameters FROM GIAnalysisTask WHERE aid_id = ? AND prediction_method = ?");
    
    $fetch_task->execute($self->{aid}, $module) 
	or $logger->logdie("Error, can't fetch analysis $module");

    # There should only be one
    my($taskid, $task_status, $parameters, $args);
    if(($taskid, $task_status, $parameters) =
       $fetch_task->fetchrow_array) {
	$self->{taskid} = $taskid;
	$self->{task_status} = $task_status;
	$self->{module} = $module;

	if($parameters) {
	    $args = decode_json $parameters;
	    $logger->trace("Parameters for $module: " . Dumper($args));
	    $self->{module_args} = $args;
	} else {
	    $self->{module_args} = {};
	}
    } else {
	$logger->logdie("Error, can't find analysis task $module");
    }

}

sub run {
    my $self = shift;
    my $module = shift;

    $self->{module} = $module;

    # Fetch the epoch seconds of when we start
    my $starttime = time;

    # Load the module information
    my $dbh = Islandviewer::DBISingleton->dbh;

    $self->fetch_module($module);
    my $task_status = $self->{task_status};
    my $args = $self->{module_args};

    # Check to see if we're rerunning and this module has already been
    # completed
    $logger->trace("Task $self->{module} for aid $self->{aid} is in state " . $REV_STATUS_MAP->{$task_status});
    if(($task_status eq $STATUS_MAP->{COMPLETE}) || ($task_status eq $STATUS_MAP->{ERROR})) {
	$logger->info("Task $self->{module} for aid $self->{aid} is already in mode " . $REV_STATUS_MAP->{$task_status} . ", not rerunning");
	# This isn't an error running it, we just don't need to rerun it,
	# so return all ok.
	return 1;
    }

    $self->set_module_status('RUNNING');
    $self->set_status('RUNNING');

    # Now we need to make our working directory
    $self->{workdir} = $self->{base_workdir} . "/$module";
    unless( -d $self->{workdir} ) {
	unless(mkdir $self->{workdir}) {
	    # Oops, we weren't able to make the workdir...
	    $self->set_status('ERROR');
	    $self->set_module_status('ERROR');
	    $logger->error("Can't make module working directory " . $self->{workdir});
	    return 0;
	}
    }

    # Move the logging over to the analysis
    $self->change_logfile();

    # Setup the needed arguments for the modules
    $args->{workdir} = $self->{workdir};
    $args->{microbedb_ver} = $self->{microbedb_ver};
    $logger->trace("Sending args: " . Dumper($args));

    my $mod_obj; my $res;
    eval {
	no strict 'refs';
	$logger->trace("Loading module $module");
	require "Islandviewer/$module.pm";
	$mod_obj = "Islandviewer::$module"->new($args);

	# How we're going to do it is the module doesn't
	# need to know about how to write results or such
	# it just reports back success or failure.  But we'll
	# pass ourself in, and provide an interface for it
	# to send results to, if it has any (things like Distance
	# do it themself)
	$res = $mod_obj->run($self->{ext_id}, $self);

	my $diff = time - $starttime;
	# A bit of a hack, but we don't want modules to run
	# too quickly.... a module should take at least 20 seconds
	if($diff < 20) {
	    $logger->trace("Module ran too quickly, pausing " . (20 - $diff) . ' seconds');
	    sleep abs(20 - $diff);
	}

    };
    if($@) {
	# We need a special case to check the exception
	# if its a failure due to connecting to the database,
	# if so we're going to throw things back in to 
	# pending so the scheduler tries again later.
	if($@ =~ /Can't connect to MySQL server/) {
	    $logger->warn("Module $module appears to have a problem connecting to the DB server, settingto pending: $@");
	    $self->set_module_status('PENDING');
	    return 1;
	}

	$self->set_module_status('ERROR');
	$logger->logdie("Can't run module $module: $@");
    }

    if($res) {
	$self->set_module_status('COMPLETE');
	return 1;
    } else {
	$self->set_module_status('ERROR');
	return 0;
    }
}

sub generate_token {
    my $self = shift;
    my $regenerate = shift;
    my $alt_aid = shift;

    my $dbh = Islandviewer::DBISingleton->dbh;

    my $fetch_analysis = $dbh->prepare("SELECT aid, token FROM Analysis WHERE aid = ?");

    my $token_aid = $self->{aid};

    $token_aid = $alt_aid
	if($alt_aid);

    $fetch_analysis->execute($token_aid);

    if(my @row = $fetch_analysis->fetchrow_array) {
        if($row[1] && !$regenerate) {
            # Do we have an existing token? Yes, return it unless we've been
	    # asked to regenerate the token
            return $row[1];
        } else {
            # Nope? Generate one and save it
            my $token = Session::Token->new->get;

            $dbh->do("UPDATE Analysis set token = ? WHERE aid = ?", undef, $token, $token_aid);
            return $token;
        }
    } else {
        # We can't find our own record?  Very, very bad.
        die "Error, can't find analysis id " . $token_aid . " when generating security token this is very bad.";
    }
}

sub record_islands {
    my $self = shift;
    my $module_name = shift;
    my @islands = @_;

    my $dbh = Islandviewer::DBISingleton->dbh;

    my $insert_island = $dbh->prepare("INSERT INTO GenomicIsland (aid_id, start, end, prediction_method, details) VALUES (?, ?, ?, ?, ?)");

    foreach my $island (@islands) {
	my $extra = scalar @{ $island } > 2 ? $island->[2] : undef;
	$insert_island->execute($self->{aid}, $island->[0], $island->[1], $module_name, $extra)
	    or $logger->logdie("Error loading island ($self->{aid}, $island->[0], $island->[1], $module_name, $extra): $DBI::errstr");
#	$insert_island->execute($self->{aid}, $island->[0], $island->[1], $self->{module})
#	    or $logger->logdie("Error loading island ($self->{aid}, $island->[0], $island->[1], $self->{module}): $DBI::errstr");
    }
}

# Fetch the islands for the current analysis
# Optionally the modules for which we want islands can be
# passed in an an array reference of module names for multiple
# or a string for a single module

sub fetch_islands {
    my $self = shift;
    my $modules = shift;

    my $stmt = "SELECT gi, start, end, prediction_method FROM GenomicIsland WHERE aid_id = ?";

    if($modules) {
	$logger->trace("Only fetching islands for module(s): " . join(',', map { qq/'$_'/ } @$modules));
	if(ref($modules) eq 'ARRAY') {
	    $logger->trace("Module list for $self->{aid} is type ARRAY");
	    $stmt .= " and prediction_method in (" . join(',', map { qq/'$_'/ } @$modules) . ')';
	} else {
	    $logger->trace("Module list for $self->{aid} is type SCALAR (we hope)");
	    $stmt .= " and prediction_method = '$modules'";
	}
    }

    my $dbh = Islandviewer::DBISingleton->dbh;

    $logger->trace($stmt);
    my $get_islands = $dbh->prepare($stmt);

    $get_islands->execute($self->{aid}) 
	or $logger->logdie("Error, can't fetch islands for analysis $self->{aid}: $DBI::errstr");
    
    my @islands;
    while(my($gi, $start, $end, $method) = $get_islands->fetchrow_array) {
	push @islands, [$gi, $start,$end,$method];
    }

    return \@islands;

}

# Write out genes associated with genomic islands, part
# of the virulence factor calculations

sub record_genes {
    my $self = shift;
    my $genes = shift;

    my $dbh = Islandviewer::DBISingleton->dbh;

    my $insert_gene = $dbh->prepare("INSERT INTO Genes (ext_id, start, end, strand, name, gene, product, locus) VALUES (?, ?, ?, ?, ?, ?, ?, ?) ON DUPLICATE KEY UPDATE id=LAST_INSERT_ID(id)");

    my $insert_island = $dbh->prepare("INSERT INTO IslandGenes (gi, gene_id) VALUES (?, ?)");

    for my $gene (@{$genes}) {
	$logger->trace("Inserting gene: " . join(',', @{$gene}));
	# Ensure we don't have any empty fields that will cause a problem inserting
	$insert_gene->execute($self->{ext_id}, 
			      $gene->[0], 
			      $gene->[1], 
			      $gene->[4], 
			      ($gene->[2] ? $gene->[2] : undef), 
			      ($gene->[5] ? $gene->[5] : undef), 
			      ($gene->[6] ? $gene->[6] : undef), 
			      ($gene->[7] ? $gene->[7] : undef))
	    or $logger->logdie("Error loading gene ($self->{ext_id}, $gene->[0], $gene->[1], $gene->[4], $gene->[2], $gene->[5], $gene->[6], $gene->[7]): $DBI::errstr");
	my $geneid = $dbh->last_insert_id(undef, undef, undef, undef);
	foreach my $gi (@{$gene->[3]}) {
	    $insert_island->execute($gi, $geneid)
		or $logger->logdie("Error loading island ($gi, $geneid): $DBI::errstr");
	}
    }
    
}

# Take a list of modules, either as an array ref
# or as a string

sub purge {
    my $self = shift;
    my $module = shift;

    $logger->info("Purging islands for aid $self->{aid} for module(s): " . Dumper($module));
    my $modules;
    if(ref($modules) eq 'ARRAY') {
	$modules = $module;
    } else {
	push @$modules, $module;
    }
    
    foreach my $m (@$modules) {
	$logger->trace("Purging islands for aid $self->{aid}, doing module: " . Dumper($m));
	$self->purge_module($m);
    }

}

sub purge_module {
    my $self = shift;
    my $module = shift;

    my $dbh = Islandviewer::DBISingleton->dbh;

    $logger->info("Purging module $module for aid $self->{aid}");

    # First remove the IslandGenes records
    $dbh->do("DELETE FROM IslandGenes WHERE gi in (SELECT gi FROM GenomicIsland WHERE aid_id = ? AND prediction_method = ?)", undef, $self->{aid}, $module) or $logger->logdie("Error purging IslandGenes for aid $self->{aid}: $DBI::errstr");

    # Then delete all the GenomicIsland records
    $dbh->do("DELETE FROM GenomicIsland WHERE aid_id = ? AND prediction_method = ?", undef, $self->{aid}, $module) or $logger->logdie("Error purging GenomicIsland for aid $self->{aid}: $DBI::errstr");

}

# Clone an analysis and return the new analysis instance
#
# Cloned jobs will default to being owner 1 - a public
# job with new custom owner

sub clone {
    my $self = shift;
    my $args = @_ ? shift : {};

    my $owner = exists $args->{owner_id} ? $args->{owner_id} : 1;
    my $default_analysis = $args->{default_analysis} ? $args->{default_analysis} : 0;

    my $dbh = Islandviewer::DBISingleton->dbh;

    $logger->info("Cloning analysis, $self->{aid}");

    # First let's clone the analysis table record
    $logger->trace("Cloning analysis record in database: " . $self->{aid});
    $dbh->do("INSERT INTO Analysis (atype, ext_id, owner_id, status, workdir, microbedb_ver, token, default_analysis) SELECT atype, ext_id, $owner, status, workdir, microbedb_ver, token, $default_analysis FROM Analysis WHERE aid = ?", undef, $self->{aid} ) or $logger->logdie("Error cloning analysis $self->{aid}: $DBI::errstr");
    my $new_aid = $dbh->last_insert_id(undef, undef, undef, undef);
    $logger->trace("New analysis id, was " . $self->{aid} . ", new id is " . $new_aid);

    if($args->{demote_old}) {
        $logger->debug("We've been told to demote the old analysis " . $self->{aid} . " from being the default");
        $dbh->do("UPDATE Analysis SET default_analysis = 0 WHERE aid = ?", undef, $self->{aid});
    }

    # Regenerate the token, we don't sharethose anymore
    $self->generate_token(1, $new_aid);

    # We could do this with triggers but we won't, see below.
    # Make the workdir for our analysis
    my $new_workdir = File::Spec->catpath(undef, $cfg->{analysis_directory}, "$new_aid");
    unless(mkdir $new_workdir) {
	# Oops, we weren't able to make the workdir...
	$logger->logdie("Oops, we weren't able to make the workdir $new_workdir for cloned analysis $new_aid");
    }

    # See if we can shorten the directory to allow moving
    # of install base in the future
    my $shortened_workdir = $new_workdir;
    if($cfg->{analysis_directory} && 
       $shortened_workdir =~ /$cfg->{analysis_directory}/) {
	$shortened_workdir =~ s/$cfg->{analysis_directory}/{{analysis_directory}}/;
	$logger->trace("Shortening workdir with analysis_directory: $shortened_workdir");
    } elsif($cfg->{workdir} &&
	    $shortened_workdir =~ /$cfg->{workdir}/) {
	$shortened_workdir =~ s/$cfg->{workdir}/{{workdir}}/;
	$logger->trace("Shortening workdir with workdir: $shortened_workdir");
    } elsif($cfg->{rootdir} && 
	    $shortened_workdir =~ /$cfg->{rootdir}/) {
	$shortened_workdir =~ s/$cfg->{rootdir}/{{rootdir}}/;
	$logger->trace("Shortening rootdir with workdir: $shortened_workdir");
    }

    $dbh->do("UPDATE Analysis SET workdir = ? WHERE aid = ?", undef, $shortened_workdir, $new_aid);

    if($args->{microbedb_ver}) {
        $dbh->do("UPDATE Analysis SET microbedb_ver = ? WHERE aid = ?", undef, $args->{microbedb_ver}, $new_aid);
    }

    # Next clone the prediction tasks, since we're only cloning completed runs
    # we can just directly copy
    $dbh->do("INSERT INTO GIAnalysisTask (aid_id, prediction_method, status, parameters, start_date, complete_date) SELECT $new_aid, prediction_method, status, parameters, start_date, complete_date FROM GIAnalysisTask WHERE aid_id = ?", undef, $self->{aid} ) or $logger->logdie("Error cloning GIAnalysisTask for new analysis $new_aid: $DBI::errstr");
    
    # Clone the islands found
    # But it's not that simple, we also need the IslandGenes records
    # which means we need to have a few loops to copy them over
    # as we encounter each island.
    my $stmt = "SELECT gi, start, end, prediction_method FROM GenomicIsland WHERE aid_id = ?";
    my $fetch_islands = $dbh->prepare($stmt) or $logger->logdie("Error preparing fetch_islands statement for cloning aid $self->{aid}:  $DBI::errstr");

    $stmt = "INSERT INTO GenomicIsland (aid_id, start, end, prediction_method) VALUES (?, ?, ?, ?)";
    my $insert_island = $dbh->prepare($stmt) or $logger->logdie("Error preparing insert_island statement for cloning aid $self->{aid}: $DBI::errstr");

    $fetch_islands->execute($self->{aid});
    $logger->trace("Cloning GenomicIsland and IslandGenes records for new analysis $new_aid");
    while(my @row = $fetch_islands->fetchrow_array) {
	# We have an island, insert it with the new aid and retrieve 
	# the new gi id
	$insert_island->execute($new_aid, $row[1], $row[2], $row[3]) or $logger->logdie("Error inserting island for cloned analysis $new_aid: $DBI::errstr");
	my $new_gi = $dbh->last_insert_id(undef, undef, undef, undef);

	# Now that we have the new gi, copy all the IslandGenes records over
	$dbh->do("INSERT INTO IslandGenes (gi, gene_id) SELECT $new_gi, gene_id FROM IslandGenes WHERE gi = ?", undef, $row[0]);
    }

    # Finally we need to clone the workdir so we have all the intermediate files
    $logger->info("Copying workdir for cloned analysis $new_aid, from $self->{base_workdir} to $new_workdir");
    my $copy_res = dircopy($self->{base_workdir}, $new_workdir);
    $logger->trace("Copied $copy_res files");

    # Our analysis should now be cloned, let's instantiate it!
    my $new_analysis_obj = Islandviewer::Analysis->new({workdir => $cfg->{analysis_directory}, aid => $new_aid});

    return $new_analysis_obj;
    
}

# Allows a modules to write back updated parameters if it sets/remembers anything

sub update_args {
    my $self = shift;
    my $args = shift;
    my $module = shift;

    my $module_to_set = $self->{module};
    if($module) {
	$module_to_set = $module;
    }

    # Load the module information
    my $dbh = Islandviewer::DBISingleton->dbh;

    my $JSON_args;
    if($args) {
	$JSON_args = to_json($args);
	$logger->trace("Updating arguments for module $module_to_set, aid $self->{aid} to $JSON_args");

	$dbh->do("UPDATE GIAnalysisTask SET parameters = ? WHERE aid_id = ? AND prediction_method = ?", {}, $JSON_args, $self->{aid}, $module_to_set);
    }

}

sub fetch_args {
    my $self = shift;
    my $module = shift;

    # Load the module information
    my $dbh = Islandviewer::DBISingleton->dbh;

    $logger->trace("Fetching arguments for module $module, aid $self->{aid}");
    my $fetch_arg = $dbh->prepare("SELECT parameters FROM GIAnalysisTask WHERE aid_id = ? AND prediction_method = ?");
    $fetch_arg->execute($self->{aid}, $module);
    if(my ($parameters) = $fetch_arg->fetchrow_array) {
	return from_json($parameters);
    }

    return {};

}

sub set_status {
    my $self = shift;
    my $status = shift;

    $status = uc $status;

    my $dbh = Islandviewer::DBISingleton->dbh;

    # Make sure its a valid status
    if($STATUS_MAP->{$status}) {
	$logger->trace("Updating analysis " .  $self->{aid} . " to status $status");
	$dbh->do("UPDATE Analysis SET status = ? WHERE aid = ?", undef, $STATUS_MAP->{$status}, $self->{aid});
    } else {
	$logger->error("Error, status $status doesn't seem to be valid");
    }
}

sub set_module_status {
    my $self = shift;
    my $status = shift;
    my $module = (@_ ? shift : $self->{module});

    my $taskid;
    if($module eq $self->{module}) {
	$taskid = $self->{taskid};
    } else {
	$taskid = $self->fetch_taskid($module);
    }

    $status = uc $status;

    my $dbh = Islandviewer::DBISingleton->dbh;

    # Make sure its a valid status
    if($STATUS_MAP->{$status}) {
	my $datestr = '';
	$datestr = ', start_date = NOW() ' if($status eq 'RUNNING');
	$datestr = ', complete_date = NOW() ' if($status eq 'COMPLETE');

	$logger->trace("Updating analysis " .  $self->{aid} . ", module " . $module . " to status $status");
	$dbh->do("UPDATE GIAnalysisTask SET status = ? $datestr WHERE taskid = ?", undef, $STATUS_MAP->{$status}, $taskid);
    } else {
	$logger->error("Error, status $status doesn't seem to be valid (module $module)");
    }
}

# Fetch the status for all the modules and return
# that set

sub fetch_module_statuses {
    my $self = shift;

    my $dbh = Islandviewer::DBISingleton->dbh;

    my $fetch_status = $dbh->prepare("SELECT status FROM GIAnalysisTask WHERE prediction_method = ? AND aid_id = ?");

    my $status_set;

    # I don't know if checking for @secondary_modules will break this...
    foreach my $mod (@modules, @secondary_modules) {
	$logger->trace("Fetching status for $mod");
	$fetch_status->execute($mod, $self->{aid}) or
	    $logger->logdie("Error running sql statement: $DBI::errstr");
	if(my @row = $fetch_status->fetchrow_array) {
	    my $status = $REV_STATUS_MAP->{$row[0]};

	    $status_set->{$mod}->{status} = $status;
	    if($mod ~~ @required_success) {
		$status_set->{$mod}->{required} = 1;
	    }
	}
    }

    return $status_set;
}

# We could use a trigger like this for the insertion in to
# the analysis table, but we're not going to, just to be
# safe, since someone in the future might not notice this
# when making a new dev version, etc of the software

# DELIMITER //    

#    CREATE TRIGGER inserName BEFORE INSERT ON name FOR EACH ROW
#    BEGIN
#        DECLARE next_ai INT;
#        SELECT auto_increment INTO next_ai
#          FROM information_schema.tables
#          WHERE table_schema = DATABASE() AND table_name = 'name';
#        SET NEW.name = CONCAT("I am number ", next_ai);
#    END //

#DELIMITER ;
