=head1 NAME

    Islandviewer::Genome_Picker

=head1 DESCRIPTION

    Object to pick nearest genomes for IslandPick

=head1 SYNOPSIS

    use Islandviewer::GenomePicker;

    $dist = Islandviewer::Distance->new({workdir => '/tmp/workdir'});
    $dist->calculate_all(version => 73, custom_replicon => $repHash);
    $distance->add_replicon(cid => 2, version => 73);
    
=head1 AUTHOR

    Matthew Laird
    Brinkman Laboratory
    Simon Fraser University
    Email: lairdm@sfu.ca

=head1 LAST MAINTAINED

    Oct 17, 2013

=cut

package Islandviewer::Genome_Picker;

use strict;
use Moose;
use Log::Log4perl qw(get_logger :nowarn);
use Data::Dumper;

#use Islandviewer::Schema;

use MicrobedbV2::Singleton;

my $cfg; my $logger; my $cfg_file;

sub BUILD {
    my $self = shift;
    my $args = shift;

    $cfg = Islandviewer::Config->config;

#    $self->{schema} = Islandviewer::Schema->connect($cfg->{dsn},
#					       $cfg->{dbuser},
#					       $cfg->{dbpass})
#	or die "Error, can't connect to Islandviewer via DBIx";

    die "Error, you must specify a microbedb version"
	unless($args->{microbedb_version} || $args->{microbedb_ver});
    $self->{microbedb_ver} = $args->{microbedb_version} if( $args->{microbedb_version} );
    $self->{microbedb_ver} = $args->{microbedb_ver} if( $args->{microbedb_ver} );

    $logger = Log::Log4perl->get_logger;

    # Setup the cutoffs for the run, we'll use the defaults
    # unless we're explicitly told otherwise
    # and yes, I used all caps, that's what it is in the original
    # code and I'm playing it safe in case of code reuse.
    $self->{max_cutoff} = defined($args->{MAX_CUTOFF}) ? $args->{MAX_CUTOFF} : $cfg->{MAX_CUTOFF};
    $self->{min_cutoff} = defined($args->{MIN_CUTOFF}) ? $args->{MIN_CUTOFF} : $cfg->{MIN_CUTOFF};
    $self->{max_compare_cutoff} = defined($args->{MAX_COMPARE_CUTOFF}) ? $args->{MAX_COMPARE_CUTOFF} : $cfg->{MAX_COMPARE_CUTOFF};
    $self->{min_compare_cutoff} = defined($args->{MIN_COMPARE_CUTOFF}) ? $args->{MIN_COMPARE_CUTOFF} : $cfg->{MIN_COMPARE_CUTOFF};
    $self->{max_dist_single_cutoff} = defined($args->{MAX_DIST_SINGLE_CUTOFF}) ? $args->{MAX_DIST_SINGLE_CUTOFF} : $cfg->{MAX_DIST_SINGLE_CUTOFF};
    $self->{min_dist_single_cutoff} = defined($args->{MIN_DIST_SINGLE_CUTOFF}) ? $args->{MIN_DIST_SINGLE_CUTOFF} : $cfg->{MIN_DIST_SINGLE_CUTOFF};
    $self->{min_gi_size} = defined($args->{MIN_GI_SIZE}) ? $args->{MIN_GI_SIZE} : $cfg->{MIN_GI_SIZE};

    my @configs = ($self->{max_cutoff}, $self->{min_cutoff}, $self->{max_compare_cutoff}, $self->{min_compare_cutoff}, $self->{max_dist_single_cutoff}, $self->{min_dist_single_cutoff}, $self->{min_gi_size});
    $logger->trace("Config options: " . join(',', @configs));
}

# Clear data structures before a rerun

sub clear_structures {
    my $self = shift;

    $logger->debug("Clearing data structures");
    $self->{dist_set} = undef;
    $self->{primary_rep_accnum} = undef;
    $self->{picked} = undef;
    $self->{find_custom_name} = undef;
}

# Find all the genomes within our range, and par it down to fit the min/max
# number of comparative genomes we're allowed
#
# Input: a rep_accnum

sub find_comparative_genomes {
    my $self = shift;
    my $rep_accnum = shift;
    my $dists = shift;

    $logger->debug("Finding comparative genomes for $rep_accnum using microbedb_ver " . $self->{microbedb_ver});

    # Clear internal structures before a run
    $self->clear_structures();

    # First, if we haven't done so already (August 2023),
    # let's get all the genomes which meet our distance
    # criteria, we'll now have a hash matching that
    if (!$dists) {
        $dists = $self->find_distance_range($rep_accnum);
    }

    # Next let's find our own strain's name
    my $name = $self->find_name($rep_accnum);

    # Start building our data structure we'll need for
    # the calculations
    $self->{dist_set}->{$rep_accnum}->{name} = $name;
    $self->{dist_set}->{$rep_accnum}->{dists} = $dists;
    $self->{primary_rep_accnum} = $rep_accnum;

    # Next let's go through all the distances we have so far, find
    # their names, and skip them if they're the same
    # This qualification if from Morgan's original code, we want
    # to skip if its the same strain just from a different
    # sequencing centre
    foreach my $cur_accnum (keys %{$dists}) {
	# Special case, we know its a microbedb identifier so
	# skip a DB hit by saying so
	my $cur_name = $self->find_name($cur_accnum, 'microbedb');

	# Is it the same strain? if not, we don't want it
	# Delete it so we don't see it later
	if($cur_name eq $name) {
	    delete $self->{dist_set}->{$rep_accnum}->{dists}->{$cur_accnum};
	    next;
	}

	# Save this for later in our dataset
	$self->{dist_set}->{$cur_accnum}->{name} = $cur_name;
    }

    # Alright, at this point we have a data structure of all
    # the genomes that match the min/max distance from our reference
    # genome, and are not different variants of the same strain.
    # We're probably looping once more time than needed, but now
    # we go through this set again and fill in all the distances to
    # each other that we don't have.
    $self->fill_in_distances();

    # Alright, the way we're going to par down the matches
    # meet max_compare_cutoff is we'll assume all are in
    # then widdle them down based on who has the minimum
    # average distance (the most clusted genome)
    # So first lets make our list of all the genomes
    # possible and set the average distance to 1
    foreach my $accnum (keys %{$self->{dist_set}->{$self->{primary_rep_accnum}}->{dists}}) {
	$self->{picked}->{$accnum} = 1;
    }

    # Do a quick sanity check, do we have at least one matching the min/max
    # single distance cutoffs? If not, we're done, bye bye.
    unless($self->check_thresholds) {
	$logger->trace("Didn't match threshold criteria");
	return undef;
    }

    # Another quick sanity check, do we have the minimum number
    # of genomes?
    # No? Return nothing, failed.
    if(scalar(keys %{$self->{dist_set}->{$self->{primary_rep_accnum}}->{dists}}) <
       $self->{min_compare_cutoff}) {

	$logger->trace("Didn't meet minimum number of genomes");
	return undef;
    }
    
    # Now let's start the trimming loop
    # Trim while we have too many results
    while(scalar(keys %{$self->{picked}}) > $self->{max_compare_cutoff}) {

	# Compute all the averages for the
	# remaining possible genomes
	$self->make_average_dists();

	# Now... next corner case, what if removing
	# the most interconnected genome will push
	# the min/max thresholds out of range? Yet at the
	# same time our count is still over the max allowed
	# matches?  Oops.  So we need to pull from the
	# bottom and keep putting that jenga piece back
	# until we can pull one that doesn't break the
	# thresholds.
	# After talking with Morgan it sounds like getting
	# down to the max comparable genomes is more 
	# important, so we'll do this thread pulling
	# and if we can't find one we'll default to
	# pulling the most interconnected (lowest
	# average distance)
	$self->pull_a_thread();
    }

    # By this point we should have a set of picked
    # genomes and (maybe) a larger list of candidate
    # but not selected genomes.  Make a structure to
    # send them back
    my $result_genomes;
    for my $cur_accnum (keys %{$self->{dist_set}->{$self->{primary_rep_accnum}}->{dists}}) {
	# Add the distance
	$result_genomes->{$cur_accnum}->{dist} = 
	    $self->{dist_set}->{$self->{primary_rep_accnum}}->{dists}->{$cur_accnum};

	# Save the name
	$result_genomes->{$cur_accnum}->{name} =
	    $self->{dist_set}->{$cur_accnum}->{name};

	# If this one is picked, label it
	if(defined($self->{picked}->{$cur_accnum})) {
	    $result_genomes->{$cur_accnum}->{picked} = 1;
        $logger->info("PICKED: " . $cur_accnum . " " . $self->{dist_set}->{$self->{primary_rep_accnum}}->{dists}->{$cur_accnum});
	}
    }

    # Finally, send back the results!
    return $result_genomes;
}

sub pull_a_thread {
    my $self = shift;

    # First lets get a sorted list of the candidate
    # genome accnums
    my @sorted_candidates = sort { $self->{picked}->{$a} <=>
				   $self->{picked}->{$b} }
                                 keys(%{$self->{picked}});

    # Loop through the candidate genomes in sorted
    # order by average distance
    for my $candidate (@sorted_candidates) {
	# Pull out a candidate and see if we still meet
	# the threshold
	if($self->check_thresholds($candidate)) {
	    # Alright, it seems safe to pull this
	    # candidate genome
	    delete $self->{picked}->{$candidate};
	    return;
	}
    }

    # If we got here we didn't find any safe
    # candidate to pull without breaking the
    # thresholds, so just pull the most connected
    # and move on
    my $candidate = shift @sorted_candidates;
    delete $self->{picked}->{$candidate};

}

# Go through all the distances and compute the average
# distance to all the other items left in the possible
# candidates

sub make_average_dists {
    my $self = shift;

    # Record our remaining candidates
    my @candidates = keys %{$self->{picked}};
    # And of course add our query genome
    push @candidates, $self->{primary_rep_accnum};

    # Now for each candidate recalculate the average distance
    foreach my $accnum (@candidates) {
	# We don't need to calculate the avg distance for the
	# query, its not on trial
	next if($accnum eq  $self->{primary_rep_accnum});

	my $sum = 0;
	# Remember how many we've found...
	my $found_count = 0;

	# Now loop through again adding all the distances together
	INNER: foreach my $against (@candidates) {
	    # That'd be silly to put outself in there
	    next INNER if($against eq $accnum);

	    # What if cvtree didn't run correctly against this
	    # pair? Ignore it.
	    if($self->{dist_set}->{$accnum}->{dists}->{$against}) {
		$sum += $self->{dist_set}->{$accnum}->{dists}->{$against};
		$found_count++;
	    }
	}

	# Alright now let's find the average and remember it
	if($found_count) {
	    $self->{picked}->{$accnum} = $sum / $found_count;
	} else {
	    $self->{picked}->{$accnum} = 0;
	}
    }

}

# Check the min and max single cutoff thresholds, to ensure
# we have a set that meets these.  Some comments from Morgan's
# original code why this is important:
#
#Setting a maxmimum distance that at least one genome has to be within
#This is to ensure that the POSITIVE DATASET is searching for events on a similar time scale
#Decreasing this will reduce our positive dataset
#
#Setting a minimum distance that at least one genome has to be over
#This is to ensure that the NEGATIVE DATASET is finding old enough conserved regions
#Increasing this will reduce our negative dataset
#
# The way we're going to do this is by comparing against the set of
# picked candidate genomes, this way the comparison gets smaller every
# cycle and we can reuse this code to check within the trimming loop
#
# And because we're using this routing in our pull_a_thread we need
# the ability to exclude a particular candidate genome, hence the
# optional skip_accnum parameter.

sub check_thresholds {
    my $self = shift;
    my $skip_accnum = ( @_ ? shift : undef );

    # Loop through all the distances associated with the query
    # rep_accnum and ensure we have at least one meeting the
    # min and max
    # Remember, we're only examining ones that are in
    # contention to be in the final pick list
    my $found_min = 0; my $found_max = 0;
    foreach my $accnum (keys %{$self->{picked}}) {
#    foreach my $accnum (keys %{$self->{data_set}->{$self->{primary_rep_accnum}}->{dists}}) {
	# Does the distance even exist? What if cvtree had failed
	next unless($self->{dist_set}->{$self->{primary_rep_accnum}}->{dists}->{$accnum});

	# We're potentially skipping candidate genomes if
	# we're calling this from pull_a_thread, so check 
	# for the optional parameter and skip
	next if(defined($skip_accnum) && ($skip_accnum eq $accnum) );

	# Have we found a distance less than the max cutoff?
	$found_max = 1
	    if($self->{dist_set}->{$self->{primary_rep_accnum}}->{dists}->{$accnum} <=
	       $self->{max_dist_single_cutoff} );

	# Have we found a distance greater than the min cutoff?
	$found_min = 1
	    if($self->{dist_set}->{$self->{primary_rep_accnum}}->{dists}->{$accnum} >=
	       $self->{min_dist_single_cutoff} );

	# We've found both, return true
	return 1
	    if($found_max && $found_min);
    }

    # We haven't found one or the other, return false
    return 0;
}

# Will return all accnum's between the cutoff values

sub find_distance_range {
    my $self = shift;
    my $rep_accnum = shift;

    my $dbh = Islandviewer::DBISingleton->dbh;

    my $sqlstmt = "SELECT rep_accnum1, rep_accnum2, distance FROM Distance WHERE (rep_accnum1 = ? OR rep_accnum2 = ?) AND distance <= $self->{max_cutoff} AND distance >= $self->{min_cutoff}";
    $logger->trace("Searching: $sqlstmt");
    my $find_dists = $dbh->prepare($sqlstmt) or 
	die "Error preparing statement: $sqlstmt: $DBI::errstr";

    $find_dists->execute($rep_accnum, $rep_accnum) or
	die "Error, can't execute query: $DBI::errstr";

    # We're going to lookup if the found replicons are still
    # valid in this version
    my $genome_utils = Islandviewer::GenomeUtils->new({microbedb_ver => $self->{microbedb_ver} });

    # Alright, now let's build a set of the distances in a data structure
    my $dists;
    while(my @row = $find_dists->fetchrow_array) {
        $logger->trace("Found row: " . Dumper(@row));
	# Find which way around the pair is, put it in the data structure
	if($row[0] eq $rep_accnum) {
            my $genome_obj = $genome_utils->fetch_genome($row[1]);
            unless($genome_obj->genome_status() eq 'READY') {
                $logger->warn("Replicon is no longer valid in microbedb " . $self->{microbedb_ver} . ": " . $row[1]);
                next;
            }
	    $dists->{$row[1]} = $row[2];
	} elsif($row[1] eq $rep_accnum) {
            my $genome_obj = $genome_utils->fetch_genome($row[0]);
            unless($genome_obj->genome_status() eq 'READY') {
                $logger->warn("Replicon is no longer valid in microbedb " . $self->{microbedb_ver} . ": " . $row[0]);
                next;
            }
	    $dists->{$row[0]} = $row[2];
	}
    }

    $logger->trace("Candidates: " . Dumper($dists));

    return $dists;
}

sub fill_in_distances {
    my $self = shift;

    # We want to go through the dist_set and for every
    # entry that doesn't have a ->{dist} entry, build one
    # but first we need to know all the identifiers we're
    # going to be comparing against
    my @accnums = keys %{$self->{dist_set}};

    my $dbh = Islandviewer::DBISingleton->dbh;
    my $sqlstmt = "SELECT rep_accnum1, rep_accnum2, distance FROM Distance WHERE rep_accnum1 = ? OR rep_accnum2 = ?";
    my $find_dists = $dbh->prepare($sqlstmt) or 
	die "Error preparing statement: $sqlstmt: $DBI::errstr";

    foreach my $accnum (@accnums) {
	# Is there already a set of distances? Skip.
	next if(defined($self->{dist_set}->{$accnum}->{dists}));

	$find_dists->execute($accnum, $accnum);
	while(my @row = $find_dists->fetchrow_array) {
	    if($row[0] eq $self->{primary_rep_accnum}) {
		# If the referenced accnum is in the first slot, we need to hunt the second
		if($row[1] ~~ @accnums) {
		    # We've found a pair!
		    $self->{dist_set}->{$accnum}->{dists}->{$row[1]} = $row[2];
		}
	    } else {
		# If the referenced accnum is in the second slot, we need to hunt the first
		if($row[0] ~~ @accnums) {
		    # We've found a pair!
		    $self->{dist_set}->{$accnum}->{dists}->{$row[0]} = $row[2];
		}

	    }
	}
    }

    # By this point we should have a set of all the distances, we're done
}

# Looks up the definition or custom genome name associated with
# a rep_accnum, takes an optional second parameter of 'custom'
# or 'microbedb' to save a DB hit if we know what type its going
# to be

sub find_name {
    my $self = shift;
    my $rep_accnum = shift;
    my $type = (@_ ? shift : 'unknown');

    unless($type eq 'microbedb' || $rep_accnum =~ /\D/) {
    # If we know we're not hunting for a microbedb genome identifier...
    # or if there are non-digits, we know custom genomes are only integers
    # due to it being the autoinc field in the CustomGenome table
    # Do this one first since it'll be faster

	# Only prep the statement once...
	unless($self->{find_custom_name}) {
	    my $dbh = Islandviewer::DBISingleton->dbh;

	    my $sqlstmt = "SELECT name from CustomGenome WHERE cid = ?";
	    $self->{find_custom_name} = $dbh->prepare($sqlstmt) or 
		die "Error preparing statement: $sqlstmt: $DBI::errstr";
	}

	$self->{find_custom_name}->execute($rep_accnum);

	# Do we have a hit? There should only be one row,
	# its a primary key
	if($self->{find_custom_name}->rows > 0) {
	    my ($name) = $self->{find_custom_name}->fetchrow_array;
	    return $name;
	}
    }

    unless($type  eq 'custom') {
    # If we know we're not hunting for a custom identifier    

        my $microbedb = MicrobedbV2::Singleton->fetch_schema;

        my $rep_results = $microbedb->resultset('Replicon')->search( {
            rep_accnum => $rep_accnum,
            version_id => $self->{microbedb_ver}
                                                                  }
            )->first;
	
	# We found a result in microbedb
	if( defined($rep_results) ) {
	    return $rep_results->definition;
	}
    }

    # This should actually never happen if we're
    # doing things right, but handle it anyways
    return 'unknown';

}

1;
