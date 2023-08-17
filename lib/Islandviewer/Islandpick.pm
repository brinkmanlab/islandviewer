=head1 NAME

    Islandviewer::Islandpick

=head1 DESCRIPTION

    Object to run Islandpick against a given genome

=head1 SYNOPSIS

    use Islandviewer::Islandpick;

    $islandpick_obj = Islandviewer::Islandpick->new({workdir => '/tmp/workdir',
                                                     microbedb_version => 80,
                                                     MIN_GI_SIZE => 8000});

    # Optional comparison rep_accnums, otherwise it uses the genome picker
    $islandpick_obj->run_islandpick($rep_accnum, @comparison_rep_accs);
    
=head1 AUTHOR

    Matthew Laird
    Brinkman Laboratory
    Simon Fraser University
    Email: lairdm@sfu.ca

=head1 LAST MAINTAINED

    Oct 16, 2013

=cut

package Islandviewer::Islandpick;

use strict;
use Moose;
use Log::Log4perl qw(get_logger :nowarn);
use File::Temp qw/ :mktemp /;
use Data::Dumper;
use File::Spec;

use Islandviewer::Schema;
use Islandviewer::Islandpick::BlastIslands;

use Islandviewer::GenomeUtils;

use MicrobedbV2::Singleton;

#Bioperl
#use Bio::Tools::Run::StandAloneBlast;
#use Bio::SearchIO;
use Bio::SeqIO;
#use Bio::Seq;
#use Bio::Range;

# local modules
use Islandviewer::Mauve;
use Islandviewer::Genome_Picker;

my $cfg; my $logger; my $cfg_file;

my $module_name = 'Islandpick';

# Method to build an instance
#
# Islandviewer::Islandpick->new({arg => value, ...});
#
# Arguments:
#
#  required:
#    workdir => '/tmp/workdir'
#    microbedb_version => 73
#  optional:
#    MAX_CUTOFF, MIN_CUTOFF, MAX_COMPARE_CUTOFF, MIN_COMPARE_CUTOFF
#    MAX_DIST_SINGLE_CUTOFF, MIN_DIST_SINGLE_CUTOFF, MIN_GI_SIZE
#
sub BUILD {
    my $self = shift;
    my $args = shift;

    $cfg = Islandviewer::Config->config;
    $cfg_file = File::Spec->rel2abs(Islandviewer::Config->config_file);

    $logger = Log::Log4perl->get_logger;

    # Save the args for later
    $self->{args} = $args;

    die "Error, work dir not specified:  $args->{workdir}"
	unless( -d $args->{workdir} );
    $self->{workdir} = $args->{workdir};

    $self->{schema} = Islandviewer::Schema->connect($cfg->{dsn},
					       $cfg->{dbuser},
					       $cfg->{dbpass})
	or die "Error, can't connect to Islandviewer via DBIx";

    die "Error, you must specify a microbedb version"
	unless($args->{microbedb_ver});
    $self->{microbedb_ver} = $args->{microbedb_ver};

    $self->{comparison_genomes} = (defined $args->{comparison_genomes} ? 
				   $args->{comparison_genomes} : undef );

    # Setup the cutoffs for the run, we'll use the defaults
    # unless we're explicitly told otherwise
    # and yes, I used all caps, that's what it is in Morgan's original
    # code and I'm playing it safe in case of code reuse.
    $self->{MAX_CUTOFF} = $args->{MAX_CUTOFF} || $cfg->{MAX_CUTOFF};
    $self->{MIN_CUTOFF} = $args->{MIN_CUTOFF} || $cfg->{MIN_CUTOFF};
    $self->{MAX_COMPARE_CUTOFF} = $args->{MAX_COMPARE_CUTOFF} || $cfg->{MAX_COMPARE_CUTOFF};
    $self->{MIN_COMPARE_CUTOFF} = $args->{MIN_COMPARE_CUTOFF} || $cfg->{MIN_COMPARE_CUTOFF};
    $self->{MAX_DIST_SINGLE_CUTOFF} = $args->{MAX_DIST_SINGLE_CUTOFF} || $cfg->{MAX_DIST_SINGLE_CUTOFF};
    $self->{MIN_DIST_SINGLE_CUTOFF} = $args->{MIN_DIST_SINGLE_CUTOFF} || $cfg->{MIN_DIST_SINGLE_CUTOFF};
    $self->{MIN_GI_SIZE} = $args->{MIN_GI_SIZE} || $cfg->{MIN_GI_SIZE};


}

# The generic run to be called from the scheduler
# magically do everything.

sub run {
    my $self = shift;
    my $accnum = shift;
    my $callback = shift;

    my @comparison_genomes;
    if(defined $self->{comparison_genomes}) {
	@comparison_genomes= split ' ', $self->{comparison_genomes};
    }

    my @islands = ();
    eval {
	@islands = $self->run_islandpick($accnum, @comparison_genomes);
    };
    if($@) {
	$logger->logdie("Error running module, $@");
    }

    # Write back the comparison genomes if we've predicted some and weren't
    # given any
    if($self->{comparison_genomes} && ! exists ($self->{args}->{comparison_genomes})) {
	$self->{args}->{comparison_genomes} = $self->{comparison_genomes};
	$callback->update_args($self->{args});
    }

    if(@islands) {
	# If we get a undef set it doesn't mean failure, just
	# nothing found.  Write the results to the callback
	# if we have any
	if($callback) {
	    $callback->record_islands($module_name, @islands);
	}
    }

    # We just return 1 because any failure for this module
    # would be in the form of an exception thrown.
    return 1;
}

# To run islandpick we'll need to do the following:
#
# Find the comparison genomes (we could be given a
# list if we're allowing picking from the web interface)
#
# Next we run the mauve alignments
#
# Then we filter down the islands we've found to unique regions
#
# Finally we do a blast screen on the results
#
# Let's make sure these functions are nice and idempotent so we
# can restart in case of failure and write out lots of intermediate
# check points

sub run_islandpick {
    my $self = shift;
    my $rep = shift;
    my @comparison_genomes = (@_ ? @_ : () );

    $logger->debug("Starting islandpick run for rep_accnum $rep");

    # If we weren't given comparison genomes, pick some
    unless(scalar(@comparison_genomes) > 0) {
	$logger->debug("No comparison genomes given, using genome_picker");
	my $picker_obj = Islandviewer::Genome_Picker->new({microbedb_version => $self->{microbedb_ver}});

    # This was historically part of 'find_comparative_genomes', but because we want to consider subsetting the candidates list,
    # we pull the candidate list ahead of time for consideration (August 2023).
    my $dists = $picker_obj->find_distance_range($rep);

    my $final_binned_dists;
    my $candidates_subset_threshold = 500;
    # If there are more candidates then the threshold, then a subset of them will be used to prevent longer runtimes.
    # Candidates will be sorted into bins (according to their distance values) and then a certain number of
    # candidates from each bin will be taken (August 2023).
    if (scalar(keys %{$dists}) > $candidates_subset_threshold) {

        $logger->info("Selecting candidates using BOTTOM-UP BINNING");
        $final_binned_dists = $self->bin_and_select_candidates($dists, $candidates_subset_threshold);
    } else {
        $logger->info("Less then " . $candidates_subset_threshold . " candidates - using all of them");
        $final_binned_dists = $dists;
    }

    unless($final_binned_dists) {
        $logger->debug("No final dists - terminating " . Dumper($final_binned_dists));
    }

    my $results = $picker_obj->find_comparative_genomes($rep, $final_binned_dists);

	# Didn't get any results, return an empty set
	unless($results) {
	    $logger->debug("No comparison genomes found");
	    return () ;
	}

	# Loop through the results
	foreach my $tmp_rep (keys %{$results}) {
	    # If it wasn't picked, we don't want it
	    next unless($results->{$tmp_rep}->{picked});

	    # Push it on the list of comparison genomes
	    push @comparison_genomes, $tmp_rep;
	}

	$logger->debug("We have " . scalar(@comparison_genomes) . " replicons to compare against. [" . join(',', @comparison_genomes) . ']');

	# What if, the unthinkable situation, we received
	# results but nothing was picked... fail.
	unless(@comparison_genomes) {
	    $logger->logdie("We received comparison genomes, but none were 'picked', oops!");
	}

	# Save the comparison genomes to our object
	$self->{comparison_genomes} = join(' ', @comparison_genomes);
    }

    # At this point we have a set of comparison genomes,
    # lets find out a little about our contestants....
    unless($self->fill_in_info($rep)) {
	$logger->logdie("Error, can't fill in info for $rep");
#	return ();
    }

    # Remember the query rep for later
    $self->{query_rep} = $rep;

    # And again, if we don't have the correct format, what's
    # the point, bail.
    unless($self->{genomes}->{$rep}->{formats}->{fna}) {
	$logger->logdie("No fna file for $rep");
#	return ();
    }

    # And while we're going through them we'll run
    # the mauve alignments
    my @island_files;
    my $num_genomes = 0;
    foreach my $tmp_rep (@comparison_genomes) {
	unless($self->fill_in_info($tmp_rep)) {
	    $logger->logdie("Unable to fill in info for genome $tmp_rep");
#	    return ();
	}

	# And again, if we don't have the correct format, what's
	# the point, bail.
	unless($self->{genomes}->{$tmp_rep}->{formats}->{fna}) {
	    $logger->logdie("No fna file for genome $tmp_rep");
#	    return ();
	}

	$logger->debug("Starting comparison against genome $tmp_rep");

	# Part of making it idempotent, don't rerun analysis 
	# we've already done.
	unless( -f "$self->{workdir}/$tmp_rep.islands.txt" && 
		-r "$self->{workdir}/$tmp_rep.islands.txt" ) {

	    # Let's keep things a little clean, make a work directory for each
	    # mauve run, but we're going to keep the results in the workdir
	    mkdir "$self->{workdir}/$tmp_rep" or 
		$logger->logdie("Error making mauve working directory $self->{workdir}/$tmp_rep");

	    my $mauve_obj = Islandviewer::Mauve->new({workdir => "$self->{workdir}/$tmp_rep/",
						     island_size => $cfg->{mauve_island_size} });

	    # Run mauve on the pair
	    my $result_obj = $mauve_obj->run("$self->{genomes}->{$rep}->{filename}.fna",
					     "$self->{genomes}->{$tmp_rep}->{filename}.fna");

	    # And save the results for later
	    $mauve_obj->serialize_results($result_obj, 
					  "$self->{workdir}/$tmp_rep.islands.txt");
	}

	$num_genomes++;
	push @island_files, "$self->{workdir}/$tmp_rep.islands.txt";
    }

    # All the mauve runs should now be done, and if they're not, no matter
    # we did our best
    # We need to load all the results we've just saved.
    # We do it this way because we want to ensure all the mauve runs were
    # successful.  We also might distribute mauve runs in the future
    my @all_unique_regions;
    foreach my $island_file (@island_files) {
	push @all_unique_regions, $self->slurp_islands($island_file);
    }

    # Let's ensure the regions are sorted
    @all_unique_regions = sort { $a->[0] <=> $b->[0] } @all_unique_regions;

    # Now push the results together taking out duplicates and mapping
    # the overlapping regions    

    my @unique_regions = $self->find_overlap($num_genomes, @all_unique_regions);

    # Now we want to do the blast screen
    my @filtered_unique_regions;
    if( scalar(@unique_regions) > 0 ) {
	eval {
	    @filtered_unique_regions = $self->blast_screen(@unique_regions);
	};
	if($@) {
	    $logger->error("Error making call to blast_screen: $@");
	}
    } else {
	$logger->debug("No islands found, skipping blast screen");
    }

    # And we should have all the coordinates of the islands
    return @filtered_unique_regions;

}

# Since there are too many candidate genomes, a subset of them will be taken using a 'binning' approach.
# Each candidate will be sorted into a bin that corresponds to it's distance value.
# Once sorting is completed, an equal number of candidates from each bin will be selected.
# It will first try to take a number of candidates equal to the number in the bin containing the fewest number of candidates.
# If the number of selected candidates is fewer then the $candidates_subset_threshold, then the process will clear the selected
# candidates and then try again with the number in the bin containing the next-fewest number of candidates,
# continuing the process until the number of candidates selected surpasses $candidates_subset_threshold.
# This whole process is called 'bottom-up binning and selection of candidates'.
sub bin_and_select_candidates {
    my $self = shift;
    my $dists = shift;
    my $candidates_subset_threshold = shift;
    
    # Distance values will be between $self->{MIN_CUTOFF} and $self->{MAX_CUTOFF}.
    # 25 bins are created between 0 and the MAX_CUTOFF value.

    my @bins = ();
    my $binned_dists;
    # Create the bins, starting at 0
    my $bin_counter = 0;
    my $bins_increment = $self->{MAX_CUTOFF}/25;
    while ($bin_counter <= $self->{MAX_CUTOFF}) {
        # Current bin is the same as the $bin_counter variable - add it to the $binned_dists hash and @bins array
        $binned_dists->{$bin_counter} = {};
        push(@bins, $bin_counter);
        $bin_counter += $bins_increment;
    }

    $logger->info("Bins array: " . Dumper(\@bins));
    $logger->info("Bins hash: " . Dumper($binned_dists));

    # Iterate through every accession and assign it to the appropriate bin based on it's distance value.
    foreach my $cur_accnum (keys %{$dists}) {
        # Starting from the 'highest' bin (at time of writing, 0.215), compare the distance value
        # to the bin value - if the distance value is greater then the bin value, it is added to the bin.
        foreach my $bin (reverse @bins) {
             if ($dists->{$cur_accnum} > $bin) {
                 $binned_dists->{$bin}->{$cur_accnum} = $dists->{$cur_accnum};
                 # Since it has been assigned to a bin, terminate the inner for loop and move onto the next accession.
                 last;
             }
         }
     }

     $logger->info("Bins with accessions: " . Dumper($binned_dists));

    # Now that accessions have been sorted into their bins, we want to find out how many accessions are in each bin before proceeding to candidate selection.
    # First, create two hashes that are inversions of each other - one of {bin name}->{number of distances in bin}, and one of {number of distance in bin}->{bin name}.
    # The goal of this is to make the 'number of candidates in bin' a key that can be used to access the actual candidate-containing bins.
    my $remaining_binned_dist_counts;
    my $remaining_binned_count_dists;
    foreach my $bin_key (@bins) {
        # Get the number of candidates in the bin
        my $bin_hash = $binned_dists->{$bin_key};
        my $bin_dist_count = scalar keys $bin_hash;

        if ($bin_dist_count != 0) {
            # Store count in a hash with the following format: {bin} -> {bin count}, eg: {0.175}->{42}
            $remaining_binned_dist_counts->{$bin_key} = $bin_dist_count;
            # Store bin in a hash with the following format: {bin count} -> {bin}, eg: {42}->{0.175}
            $remaining_binned_count_dists->{$bin_dist_count} = $bin_key;
        }
    }
    $logger->info("Remaining Binned Dist Counts: " . Dumper($remaining_binned_dist_counts));
    $logger->info("Remaining Binned Count Dists (inversion of above hash): " . Dumper($remaining_binned_count_dists));

    # Create an array ordered by highest number of distances.
    my @sorted_bins_by_number_dists = sort {$a <=> $b} keys $remaining_binned_count_dists;
    $logger->info("Sorted bins by number of dists (lowest to highest):" . Dumper(\@sorted_bins_by_number_dists));


    # OK - we have sorted all candidates into bins, and created an array organized by 'number of distance values in bin'
    # that starts with the bin containing the lowest non-zero number of distances.
    # Now we proceed to 'bottom-up smart binning'

    my $total_candidates = 0;
    my $final_binned_dists;
    # Because we are first going to try and remove a number of candidates from each bin equal to the number in the bin with the lowest number of candidates,
    # the values in @sorted_bins_by_number_dists are thresholds, which is why each element is named $bin_max_num_candidates.
    foreach my $bin_max_num_candidates (@sorted_bins_by_number_dists) {
        $logger->info("Trying bottom-up filling with: " . $bin_max_num_candidates . " candidates taken from each bin");

        # Now that we have the maximum number of candidates to try and take from each bin, we go ahead and do so.
        # $bin_key would be equivalent to 0.175 here, based on the above example.
        # For each bin...
        foreach my $bin_key (keys %{$remaining_binned_dist_counts}) {

            my $bin_count = 0;
            # Select candidates from the bin (taken randomly because it is a Perl hash) until we hit $bin_max_num_candidates number of candidates.
            foreach my $candidate_accession (keys %{$binned_dists->{$bin_key}}) {

                # Add candidate to $final_binned_dists hash
                $logger->info("Adding " . $candidate_accession . " " . $binned_dists->{$bin_key}->{$candidate_accession});
                $final_binned_dists->{$candidate_accession} = $binned_dists->{$bin_key}->{$candidate_accession};

                $total_candidates++;
                $bin_count++;

                # We have enough candidates from this bin - terminate the inner loop and move onto the next bin.
                if ($bin_count >= $bin_max_num_candidates) {
                    last;
                }
            }
        }

        # We have enough candidates - return $final_binned_dists
        if ($total_candidates >= $candidates_subset_threshold) {
            $logger->info("Found enough candidates - moving on.");
            $logger->info("Final candidates list: " . Dumper($final_binned_dists));
            $logger->info("Final candidates count: " . $total_candidates);
            return $final_binned_dists;
        } else {
            # We do not have enough candidates - clear the $final_binned_dists data strcture and reset the $total_candidates counter to 0
            $logger->info("Insufficient candidates - " . $total_candidates . ") - deleting: " . Dumper($final_binned_dists));
            $total_candidates = 0;
            %$final_binned_dists = ();
        }
    }
}

# We're going to parse the genbank file on the fly and look for
# matches in the virulence factor table, we're doing it this way
# for two reasons, first because we're no longer going to put
# custom genomes in to microbedb, and second I plan to alter
# microbedb in the future so genomes aren't loaded either,
# it's a huge waste of space

sub find_virulence {
    my $self = shift;
    my $gbk_file = shift;

    # Get a handle to the db
    my $dbh = Islandviewer::DBISingleton->dbh;
    my $sqlstmt = "SELECT source FROM virulence WHERE protein_accnum = ?";
    my $lookup_virulence = $dbh->prepare($sqlstmt) or 
		die "Error preparing statement: $sqlstmt: $DBI::errstr";

    # Open the genbank file for reading through bioperl
    my $seqio = Bio::SeqIO->new( -file => $gbk_file );
    
    # Loop through the sequences
    my @all_regions;
    while(my $seq_obj = $seqio->next_seq) {
	# Then we have to loop through the features for each
	# sequence
	FEATURE: for my $feat_obj ($seq_obj->get_SeqFeatures) {
	    my @ids;
	    # We only want CDS tags
	    next unless($feat_obj->primary_tag eq 'CDS');
	    my $start = $feat_obj->location->start;
	    my $end = $feat_obj->location->end;
	    push @ids, $feat_obj->get_tag_values('protein_id')
		if ($feat_obj->get_tag_values('protein_id'));

	    # Now we need to look up in the database if this
	    # protein has an associated virulence factor
	    for my $id (@ids) {
		$lookup_virulence->execute($id);
		if(my ($source) = $lookup_virulence->fetchrow_array) {
		    my $vir = {start => $start,
			       end => $end,
			       source => $source };
		    push @all_regions, $vir;
		    next FEATURE;
		}
	    }
	}
    }

    return @all_regions;
}

sub blast_screen {
    my $self = shift;
    my @unique_regions = @_;
    my @tmpfiles;

    # Let's ensure the regions are sorted
    @unique_regions = sort { $a->[0] <=> $b->[0] } @unique_regions;

    # Check we actually have an fna version of our query
    unless($self->{genomes}->{$self->{query_rep}}->{formats}->{fna}) {
	$logger->error("Error, we don't have an fna for our query rep $self->{query_rep}");
	die "Error, we don't have an fna for our query rep $self->{query_rep}";
    }

    # Create a temporary fna file that does not contain the GI regions
    my $tmp_query_fna = $self->_make_tempfile();
    push @tmpfiles, $tmp_query_fna;

    my $out_query     = Bio::SeqIO->new(
            -file   => ">$tmp_query_fna",
            -format => 'Fasta'
        );

    # Open the query fna file for reading and get the sequence
    my $in = Bio::SeqIO->new(
            -file   => "$self->{genomes}->{$self->{query_rep}}->{filename}.fna",
            -format => 'Fasta'
        );
    my $contig_seq = $in->next_seq();

    my $start = 1;
    my $end;
    my $new_seq = '';
    # Go through and slice out the pudative islands
    foreach my $gi (@unique_regions) {
	$end = $gi->[0];
	$new_seq .= $contig_seq->subseq( $start, $end );

	$start = $gi->[1];
    }
    $end = $contig_seq->length();
    $new_seq .= $contig_seq->subseq( $start, $end );

    # And write out the new sequence file
    my $new_contig = new Bio::Seq(-seq =>$new_seq, -display_id => $contig_seq->id());
    $out_query->write_seq($new_contig);

    # Next we need all the names of the genomes we'll be running against
    my @fna_files;
    foreach my $rep (keys $self->{genomes}) {
	# We obviously don't want to blast against ourself
	next if($rep eq $self->{query_rep});

	# We can't blast against it if we don't have
	# the fna version
	unless($self->{genomes}->{$rep}->{formats}->{fna}) {
	    $logger->warn("There's no fna file for $rep");
	    next;
	}

	push @fna_files, "$self->{genomes}->{$rep}->{filename}.fna";
    }

    # Make a blast screen object
    my $blast_obj = Islandviewer::Islandpick::BlastIslands->new({workdir => $self->{workdir},
								 island_size => $self->{MIN_GI_SIZE}});

    my $joint_fna = $self->_make_tempfile();
    my @filtered_unique_regions;

    push @tmpfiles, $joint_fna;
    eval {
	$blast_obj->create_blast_db($joint_fna, $tmp_query_fna, @fna_files);
	@filtered_unique_regions = $blast_obj->run($joint_fna,
						      "$self->{genomes}->{$self->{query_rep}}->{filename}.fna",
						      @unique_regions);
    };
    if($@) {
	$logger->error("Error running blast filter: $@");
    }

    # And cleanup after ourself
    if($cfg->{clean_tmpfiles}) {
	$logger->trace("Cleaning up temp files for Islandpick");
    $self->_remove_tmpfiles(@tmpfiles);
    }

    # And return the results....
    return @filtered_unique_regions;

}

# Pull the island file from mauve back in, round about, yes,
# but this is more for checkpointing

sub slurp_islands {
    my $self = shift;
    my $island_file = shift;

    return undef unless(-f $island_file &&
			-r $island_file);

    open(ISLANDS, "<$island_file") or
	die "Error, can't read island_file $island_file, this should never happen: $!";

    my @islands;
    while(<ISLANDS>) {
	chomp;
	my ($start, $end) = split "\t";
	push(@islands, [ $start, $end ]);
    }

    close ISLANDS;

    return @islands;
}

#Finds unique regions that are unique in all of the pairwise comparisions
#Note: the code in this is badly written, but I never got around to rewriting it!
#
# We're reusing Morgan's original code, it's not *that* bad, I guess...

sub find_overlap {
    my $self = shift;
    my $num_genomes = shift;
    my @all_unique_regions = @_;

    my $i = 0;
    my ( @start, @end );

    foreach (@all_unique_regions) {
        ( $start[$i], $end[$i] ) = ( $_->[0], $_->[1] );
        $i++;
    }
    my ( $found, $temp_end, @finalstart, @finalend );
    foreach my $temp_start (@start) {
        $found    = 0;
        $temp_end = undef;
        for ( my $j = 0; $j < @start; $j++ ) {
            if ( $temp_start >= $start[$j] && $temp_start < $end[$j] ) {
                $found++;

                #Keeps track of the nearest end point of overlapping gaps
                if ( !( defined($temp_end) ) || $end[$j] < $temp_end ) {
                    $temp_end = $end[$j];
                }

                #found gap region that is present across all genomes
                if ( $found >= $num_genomes ) {

                    #Check if region was already found
                    my $inlist = 0;
                    for ( my $k = 0; $k < @finalstart; $k++ ) {
                        if (   $finalstart[$k] == $temp_start
                            && $finalend[$k] == $temp_end )
                        {
                            $inlist =
                              1;     #set the flag to indicate that the gap is already in the list
                            last;    #exit the for loop
                        }
                    }

                    #if not in list already add it to the list
                    if ( !($inlist) ) {
                        push( @finalstart, $temp_start );
                        push( @finalend,   $temp_end );
                    }
                    last;            # exits the for loop once overlap is found in all alignments
                }
            }
        }
    }
    my @unique_regions;
    for ( my $i = 0; $i < @finalstart; $i++ ) {
        push( @unique_regions, [ $finalstart[$i], $finalend[$i] ] );
    }
    return @unique_regions;

}

# For a genome, look it up and fill in the information
# to our internal data structure
# Let the caller know if we found all the information
# properly

sub fill_in_info {
    my $self = shift;
    my $accnum = shift;

#    my ($name, $filename, $format_str) = $self->lookup_genome($accnum);
    my $genome_obj = Islandviewer::GenomeUtils->new({microbedb_ver => $self->{microbedb_ver} });
    my($name,$filename,$format_str) = $genome_obj->lookup_genome($accnum);

    # We're going to require that the comparison genomes
    # can be looked up, otherwise, what's the point?
    return 0 unless($filename && $format_str);
    
    $self->{genomes}->{$accnum}->{name} = $name;
    $self->{genomes}->{$accnum}->{filename} = $filename;

    # Break out the formats available
    my @formats = split /\s+/, $format_str;
    foreach my $f (@formats) {
	# Remove leading .
	$f =~ s/^\.//;
	$self->{genomes}->{$accnum}->{formats}->{$f} = 1;
    }

    return 1;

}

# Lookup an identifier, determine if its from microbedb
# or from the custom genomes.  Return a package of
# information such as the base filename
# We allow to say what type it is, custom or microbedb
# if we know, to save a db hit

sub lookup_genome {
    my $self = shift;
    my $rep_accnum = shift;
    my $type = (@_ ? shift : 'unknown');

    unless($rep_accnum =~ /\D/ || $type eq 'microbedb') {
    # If we know we're not hunting for a microbedb genome identifier...
    # or if there are non-digits, we know custom genomes are only integers
    # due to it being the autoinc field in the CustomGenome table
    # Do this one first since it'll be faster

	# Only prep the statement once...
	unless($self->{find_custom_name}) {
	    my $dbh = Islandviewer::DBISingleton->dbh;

	    my $sqlstmt = "SELECT name, filename,formats from CustomGenome WHERE cid = ?";
	    $self->{find_custom_name} = $dbh->prepare($sqlstmt) or 
		die "Error preparing statement: $sqlstmt: $DBI::errstr";
	}

#	$self->{find_custom_name}->execute($rep_accnum);

	$self->{find_custom_name}->execute($rep_accnum);

	# Do we have a hit? There should only be one row,
	# its a primary key
	if($self->{find_custom_name}->rows > 0) {
	    my ($name,$filename,$formats) = $self->{find_custom_name}->fetchrow_array;
            # Expand filename
	    if($filename =~ /{{.+}}/) {
		$filename =~ s/{{([\w_]+)}}/$cfg->{$1}/eg;
	    }

	    return ($name,$filename,$formats);
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

	    return ($rep_results->definition, File::Spec->catpath(undef, $rep_results->genomeproject->gpv_directory, $rep_results->file_name) ,$rep_results->file_types);
	}
    }

    # This should actually never happen if we're
    # doing things right, but handle it anyways
    return ('unknown',undef,undef);

}

# Make a temp file in our work directory and return the name

sub _make_tempfile {
    my $self = shift;

    # Let's put the file in our workdir
    my $tmp_file = mktemp($self->{workdir} . "/blasttmpXXXXXXXXXX");
    
    # And touch it to make sure it gets made
    `touch $tmp_file`;

    return $tmp_file;
}

sub _remove_tmpfiles {
    my $self = shift;
    my @tmpfiles = @_;

    foreach my $file (@tmpfiles) {
	unless(unlink $file) {
	    $logger->error("Can't unlink file $file: $!");
	}
    }
}

1;
