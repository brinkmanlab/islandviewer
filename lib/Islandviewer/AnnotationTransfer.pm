=head1 NAME

    Islandviewer::AnnotationTransfer

=head1 DESCRIPTION

    Object to run the annotation transfer on a genome from
    similar genomes within the same family

=head1 SYNOPSIS

    use Islandviewer::AnnotationTransfer;

    $annotationtransfer_obj = Islandviewer::AnnotationTransfer->new({workdir => '/tmp/workdir',
                                         microbedb_version => 80});

=head1 AUTHOR

    Matthew Laird
    Brinkman Laboratory
    Simon Fraser University
    Email: lairdm@sfu.ca

=head1 LAST MAINTAINED

    July 22, 2015

=cut

package Islandviewer::AnnotationTransfer;

use strict;
use Moose;
use Log::Log4perl qw(get_logger :nowarn);
use File::Temp qw/ :mktemp /;
use File::Spec;
use Scalar::Util qw(reftype);

use Data::Dumper;

use Bio::SeqIO; 

use Islandviewer::DBISingleton;
use Islandviewer::Genome_Picker;
use Islandviewer::GenomeUtils;
use Islandviewer::Blast;
use Islandviewer::Constants qw(:DEFAULT $STATUS_MAP $REV_STATUS_MAP $ATYPE_MAP);

use MicrobedbV2::Singleton;

my $module_name = 'AnnotationTransfer';

my $logger; my $cfg;

sub BUILD {
    my $self = shift;
    my $args = shift;

    $cfg = Islandviewer::Config->config;

    $logger = Log::Log4perl->get_logger;

    $self->{microbedb_ver} = (defined($args->{microbedb_ver}) ?
			      $args->{microbedb_ver} : undef );

    unless($self->{microbedb_ver}) {
	my $microbedb = MicrobedbV2::Singleton->fetch_schema;
	$self->{microbedb_ver} = $microbedb->current();
	
	$logger->info("Microbedb version not defined, using current: " . $self->{microbedb_ver})
    }

    $self->{ref_accnum} = (defined($args->{ref_accnum}) ?
			      $args->{ref_accnum} : undef );
    
    die "Error, work dir not specified:  $args->{workdir}"
	unless( -d $args->{workdir} );
    $self->{workdir} = $args->{workdir};

    # We'll need these later...
    my $dbh = Islandviewer::DBISingleton->dbh;

    $self->{find_by_coord} = $dbh->prepare("SELECT id from Genes WHERE ext_id = ? AND start = ? AND end = ?") or 
	$logger->logdie("Error preparing find_by_coord statement: $DBI::errstr");

    $self->{find_by_ref} = $dbh->prepare("SELECT id from Genes WHERE ext_id = ? AND name = ?") or
	$logger->logdie("Error preparing find_by_ref statement: $DBI::errstr");

    $self->{check_curated} = $dbh->prepare("SELECT rep_accnum FROM virulence_curated_reps WHERE rep_accnum=?") or
        $logger->logdie("Error preparing check_curated statement: $DBI::errstr");
    
}

sub run {
    my $self = shift;
    my $accnum = shift;
    my $callback = shift;

    # remove annotations from virulence_mapped
    $self->clear_annotations($accnum);
    
    # Transfer genes in virulence table to virulence_mapped
    $self->transfer_curated($accnum);

    $self->{check_curated}->execute($accnum);
    if(my @row = $self->{check_curated}->fetchrow_array) {
        $logger->info("This genomes ($accnum) is a reference genome, we won't run the blast/rbb check on it");
        return;
    }

    # Find all the comparison genomes within the approved distance and
    # who's names match the criteria
    my $comparison_genomes = $self->find_comparison_genomes($accnum);

    unless($comparison_genomes) {
	$logger->error("We didn't get any comparison genomes back for $accnum, aborting.");
	return;
    }

    # And now start blasting and transfering the annotations
    my $all_rbbs = {};
    foreach my $ref_accnum (@$comparison_genomes) {
        # Find the RBBHs for a single genome
        my $found_rbbs;
        
        eval {
            $found_rbbs = $self->transfer_single_genome($accnum, $ref_accnum);
        };
        if($@) {
            $logger->error("Error transfering genome $ref_accnum for $accnum: $@");
            next;
        }

        # Now integrate those in to the master list
        foreach my $ref (keys %{$found_rbbs}) {
            if($all_rbbs->{$ref}) {
                $logger->trace("Found $ref in the master set, integrating...");

                foreach my $item (@{$found_rbbs->{$ref}}) {
                    push $all_rbbs->{$ref}, $item
                        unless(grep $_ eq $item, $all_rbbs->{$ref});
                }

            } else {
                $logger->trace("Haven't seen $ref before, copying over to master set");
                $all_rbbs->{$ref} = $found_rbbs->{$ref};
            }
        }
    }

    $logger->info("All RBBS FOUND:");
    $logger->info(Dumper($all_rbbs));

    # And here we add them to the database, again checking the
    # database first for duplicates
    eval {
        $self->update_database($accnum, $all_rbbs);
    };
    if($@) {
        $logger->error("Error updating the database for genome $accnum");
    }

    # Send back the genomes we used to do the annonation
    # transfer
    return $comparison_genomes;

}

# Transfer all the annotated genomes from the virulence table
# to the virulence_mapped table for a single genome

sub transfer_curated {
    my $self = shift;
    my $accnum = shift;

    my $dbh = Islandviewer::DBISingleton->dbh;

    $logger->info("Transferring curated annotations from $accnum to mapped table");

    my $find_virulence = $dbh->prepare("INSERT INTO virulence_mapped (gene_id, ext_id, protein_accnum, external_id, source, type, flag, pmid, date) SELECT G.id, G.ext_id, V.protein_accnum, V.external_id, V.source, V.type, V.flag, V.pmid, V.date FROM Genes AS G, virulence AS V WHERE G.name = V.protein_accnum and G.ext_id = ?");

    $find_virulence->execute($accnum) or
	$logger->logdie("Error transferring curated annotations: $DBI::errstr");

}

sub clear_annotations {
    my $self = shift;
    my $accnum = shift;

    $logger->info("Purging existing annotations for $accnum");

    my $dbh = Islandviewer::DBISingleton->dbh;

    $dbh->do("DELETE FROM virulence_mapped WHERE ext_id = ?", undef, $accnum) or
	$logger->logdie("Error clearing annotations: $DBI::errstr");

}

# Find all the genomes we want to transfer annotations from

sub find_comparison_genomes {
    my $self = shift;
    my $accnum = shift;

    # Get a GenomeUtils object so we can do lookups
    my $genome_utils = Islandviewer::GenomeUtils->new({microbedb_ver => $self->{microbedb_ver} });
    
    $logger->debug("Finding candidate genomes for accnum $accnum");
    
    # We're going to reuse the genome picker code for finding candidates
    my $picker_obj = Islandviewer::Genome_Picker->new({workdir => $self->{workdir},
						       microbedb_ver => $self->{microbedb_ver},
						       MIN_CUTOFF => 0,
						       MAX_CUTOFF => 0.14 });

    my $dists = $picker_obj->find_distance_range($accnum);

    # Find ourself and then filter down our name for the comparison step
    my $genome_obj = $genome_utils->fetch_genome($accnum);
    unless($genome_obj->genome_status() eq 'READY') {
	$logger->error("Failed in fetching genome object for $accnum");
	return 0;
    }

    # Stash the name of the main genome object
    my $definition = $self->filter_name($genome_obj->name());
    $logger->info("Our shortened name: $definition");

    # Now the real work, go through the candidates and see if they match the 
    # name patterns we're allowing
    my @run_shortlist;
    foreach my $cur_accnum (keys %{$dists}) {
	$logger->info("Examining: $cur_accnum, dist: " . $dists->{$cur_accnum});

	$self->{check_curated}->execute($cur_accnum);
	unless(my @row = $self->{check_curated}->fetchrow_array) {
	    $logger->info("Not in curated set, skipping");
	    next;
	}

        $logger->debug("In curated set, $cur_accnum");

	my $cur_genome_obj = $genome_utils->fetch_genome($cur_accnum);
	unless($cur_genome_obj->genome_status() eq 'READY') {
	    $logger->error("Failed in fetching genome object for $cur_accnum");
	    next;
    }

	# Grab and shorten the name of our current candidate
	my $cur_def = $self->filter_name($cur_genome_obj->name());
	$logger->info("Candidate's name: $cur_def");

	my $okay = 0;
        if($genome_obj->atype() eq $ATYPE_MAP->{custom}) {
	    # We're disabling annotation transfer for now on
	    # custom genomes until we find a better way to check distance and genus
            $okay = 0;
#            $okay = 1;
        } else {
            $okay = $self->check_names_match($definition, $cur_def);
        }
	
	if($okay) {
	    push @run_shortlist, $cur_accnum;
	    $logger->info("Name passes, using $cur_accnum for transfer");
	}
    }

    return \@run_shortlist;
}

sub transfer_single_genome {
    my $self = shift;
    my $accnum = shift;
    my $ref_accnum = shift;

    # Get a GenomeUtils object so we can do lookups
    my $genome_utils = Islandviewer::GenomeUtils->new({microbedb_ver => $self->{microbedb_ver} });

    my $query_file = $self->make_vir_fasta($ref_accnum);

    # Fetch the query genome object, and fasta file
    my $genome_obj = $genome_utils->fetch_genome($accnum);
    unless($genome_obj->genome_status() eq 'READY') {
	$logger->logdie("Failed in fetching genome object for $accnum, this shouldn't happen!");
    }

    my $subject_filename = $genome_obj->filename() . '.faa';
    $logger->trace("Fasta file for subject genome $accnum should be: $subject_filename");
    unless(-f $subject_filename) {
        $logger->logdie("Error, can't find file for subject genome $accnum: $subject_filename");
    }

    # Fetch the referencd genome object, and fasta file
    my $ref_genome_obj = $genome_utils->fetch_genome($ref_accnum);
    unless($ref_genome_obj->genome_status() eq 'READY') {
	$logger->logdie("Failed in fetching genome object for $ref_accnum, this shouldn't happen!");
    }

    my $ref_filename = $ref_genome_obj->filename() . '.faa';
    $logger->trace("Fasta file for ref genome $ref_accnum should be: $ref_filename");
    unless(-f $ref_filename) {
        $logger->logdie("Error, can't find file for ref genome $ref_accnum: $ref_filename");
    }

    my $blast_obj = new Islandviewer::Blast({microbedb_ver => $self->{microbedb_ver},
                                             workdir => $self->{workdir},
                                             db => $subject_filename,
                                             query => $query_file,
                                             evalue => 1e-10,
                                             outfmt => 5,
                                             seg => 'no',
                                             K => 3
                                            }
        );

    my $vir_hits = $blast_obj->run($query_file, $subject_filename);

    # Now we're going to go through our hits and find all the 
    # accessions for the proteins so we can make a sub-fasta file
    # of those proteins for the RBB run
    my @accs;
    foreach my $hit (keys %$vir_hits) {
        my ($type, $acc) = split /\|/, $hit;

        push @accs, $acc;
    }
    $logger->trace("Found protein accessions: " . Dumper(@accs));
    my $subject_fasta_file = $self->_make_tempfile();
    my $seq_found = $genome_utils->make_sub_fasta($genome_obj, $subject_fasta_file, @accs);
    $logger->info("Made fasta file of blast hits for rbb: $subject_fasta_file, num seq found: $seq_found");

    $logger->info("Doing blast of subject hit proteins ($subject_fasta_file) against reference genome ($ref_filename)");
    my $reverse_hits = $blast_obj->run($subject_fasta_file, $ref_filename);

    $logger->trace(Dumper($reverse_hits));

    # We have the forward and reverse BLAST, now we need to go through
    # the hits and see if we have any RBBHs

    my $found_rbbs = {};
    foreach my $vir_hit (keys %$vir_hits) {
        # Get the refseq accession(s) of everything that hits
        # this protein, then see if we have a reverse. If
        # we do, mark it down for later entry in to the
        # database.
        my $query_accnums = [];
        if(reftype $vir_hits->{$vir_hit} eq 'ARRAY') {
            $query_accnums = $vir_hits->{$vir_hit};
        } else {
            push @{$query_accnums}, $vir_hits->{$vir_hit};
        }
        $logger->trace("For hit $vir_hit found query accessions " . Dumper($query_accnums));

        foreach my $query_accnum (@{$query_accnums}) {
            # Pull apart the header line for the query to get the
            # accession piece only
            $logger->trace("Examining protein for RBB: " . Dumper($query_accnum));
            my $header = $genome_utils->split_header($query_accnum);
            $logger->trace("Split header: " . Dumper($header));

            unless(defined $header->{ref}) {
                $logger->error("We don't have a refseq accession for this protein, this is very bad");
                next;
            }

            my $ref_key = "ref|" . $header->{ref};
            $logger->debug("Looking up if we have a RBB for $ref_key");
            if($reverse_hits->{$ref_key}) {
                $logger->trace("Found key, let's see if the reverse mapping exists: " . Dumper($reverse_hits->{$ref_key}));
                # There were multiple hits, it's an array...
                if(reftype $reverse_hits->{$ref_key} eq 'ARRAY') {
                    foreach my $rev_hit (@{ $reverse_hits->{$ref_key} }) {
                        if($rev_hit =~ /$vir_hit/) {
                            $logger->info("Found an RBB! $ref_key and $vir_hit (in $rev_hit)");
                            push @{ $found_rbbs->{$vir_hit} }, $query_accnum;
                            last;
                        }
                    }
                } else {
                    if($reverse_hits->{$ref_key} =~ /$vir_hit/) {
                        $logger->info("Found an RBB! $ref_key and $vir_hit (in " . $reverse_hits->{$ref_key} . ")");
                        push @{ $found_rbbs->{$vir_hit} }, $query_accnum;
                    }
                }
            }
        }   
    }

    $logger->debug("RBBS FOUND:");
    $logger->debug(Dumper($found_rbbs));

    $logger->info("Cleaning up temp files for this iteration.");
    $blast_obj->_clean_tmp();

    return $found_rbbs;

}

# For writing the results to the database we're going
# to need some mappings, for custom genomes we'll be looking
# them up via coordinate so we need to slurp the
# fasta file and find this information. It seems
# a little wasteful to do it this way, but such is life.

sub fetch_fasta_headers {
    my $self = shift;
    my $accnum = shift;

    # Get a GenomeUtils object so we can do lookups
    my $genome_utils = Islandviewer::GenomeUtils->new({microbedb_ver => $self->{microbedb_ver} });

    # Fetch the query genome object, and fasta file
    my $genome_obj = $genome_utils->fetch_genome($accnum);
    unless($genome_obj->genome_status() eq 'READY') {
	$logger->logdie("Failed in fetching genome object for $accnum, this shouldn't happen!");
    }

    my $subject_filename = $genome_obj->filename() . '.faa';
    $logger->trace("Fasta file for subject genome $accnum should be: $subject_filename");
    unless(-f $subject_filename) {
        $logger->logdie("Error, can't find file for subject genome $accnum: $subject_filename");
    }

    $logger->trace("Opening file with bioperl: $subject_filename");
    my $in = Bio::SeqIO->new(
	-file    => $subject_filename,
	-format  => 'FASTA'
	);

    my $headers;
    while( my $seq = $in->next_seq() ) {
	# Split the display id in to piece
	my $pieces = $genome_utils->split_header($seq->display_id);

	if($pieces->{ref}) {
	    # If we have a header with an accession, add it to the lookup table
	    $headers->{$pieces->{ref}} = $pieces;
	}
    }

    return $headers;
}

# Go through the hash of rbb results and
# add them to the database. This is going to
# be complicated because we have to separate
# each out by reference accession, then
# within that group see if we have that
# accession in the database already to append
# the new records.

sub update_database {
    my $self = shift;
    my $accnum = shift;
    my $blast_results = shift;

    # Get a GenomeUtils object so we can do lookups
    my $genome_utils = Islandviewer::GenomeUtils->new({microbedb_ver => $self->{microbedb_ver} });

    # Fetch all the fasta headers from our subject genome so
    # we can look it up in the database by coordinate if needed
    my $headers = $self->fetch_fasta_headers($accnum);

    my $dbh = Islandviewer::DBISingleton->dbh;

    my $update_vir_record = $dbh->prepare("REPLACE INTO virulence_mapped (gene_id, ext_id, protein_accnum, external_id, source, type, flag, pmid) VALUES (?, ?, ?, ?, ?, ?, ?, ?)");

    foreach my $acc (keys %{$blast_results}) {
        $logger->trace("Updating record for accession $acc");

        my $acc_mapping = $genome_utils->split_header($acc);
        $logger->trace("Split header: " . Dumper($acc_mapping));
        unless(defined $acc_mapping->{ref}) {
            $logger->error("No accession for this accession? How could that happen? $acc");
            next;
        }
        
        my $ref_accnums = {};
        # Now we loop through all the RBBs found
        # for this accession, building our
        # data structure of potential duplicates

        foreach my $ref_row (@{$blast_results->{$acc}}) {
            $logger->trace("Found row $ref_row");

            my $row_pieces = $genome_utils->split_header($ref_row);
            $logger->trace("Split header: " . Dumper($row_pieces));

            unless(defined $row_pieces->{ref}) {
                $logger->error("No accession for this rbb? How could that happen? $ref_row");
                next;
            }

            # Push the record on to the structure so we can save this information
            # Filter for duplicates
            push @{$ref_accnums->{$row_pieces->{ref}}->{flag}}, $ref_row
                unless(grep $_ eq $ref_row, @{$ref_accnums->{$row_pieces->{ref}}->{flag}});

            # If we have a pmid, remember that
            if(defined $row_pieces->{pmid} && !(grep $_ eq $row_pieces->{pmid}, @{$ref_accnums->{$row_pieces->{ref}}->{pmid}})) {
                push @{$ref_accnums->{$row_pieces->{ref}}->{pmid}}, $row_pieces->{pmid};
            }

        }

        # At this point we should have all the RBBs grouped by
        # unique accession number from the reference hit.
        # Let's replace or insert any new record. The reason we're
        # replacing and not updating is because this should be
        # the definitive source for any genome we're transfering
        # annotations from, if we didn't find it now, it means
        # obviously that genome is gone from the reference set
        # and shouldn't be in the virulence table any longer.
        foreach my $ref_accnum (keys %{$ref_accnums}) {
            my $flag = (defined $ref_accnums->{$ref_accnum}->{flag} ?
                        join(';', @{$ref_accnums->{$ref_accnum}->{flag}}) :
                        undef);
            my $pmid = (defined $ref_accnums->{$ref_accnum}->{pmid} ?
                        join(';', @{$ref_accnums->{$ref_accnum}->{pmid}}) :
                        undef);

            $logger->trace("Updating virulence table: " . $acc_mapping->{ref} . ", $ref_accnum, BLAST, virulence, $flag, $pmid");
	    my $gene_id = $self->find_gene($accnum, $headers->{$acc_mapping->{ref}});

	    if($gene_id) {
		$update_vir_record->execute($gene_id,
					    $accnum,
					    ($acc_mapping->{ref} =~ /^UN_\d+\.0/ ? undef : $acc_mapping->{ref}),
					    $ref_accnum,
					    'BLAST',
					    'virulence',
					    $flag,
					    $pmid);
	    } else {
		$logger->error("We weren't able to find a gene id for the record: " . $acc_mapping->{ref} . ", $ref_accnum, BLAST, virulence, $flag, $pmid");
	    }
        }
    }
}

sub find_gene {
    my $self = shift;
    my $accnum = shift;
    my $gene_header = shift;
    
    if($gene_header->{start} && $gene_header->{end}) {
	$logger->trace("Looking up by coord: $accnum, " . $gene_header->{start} . ", " . $gene_header->{end});
	$self->{find_by_coord}->execute($accnum,
					$gene_header->{start},
					$gene_header->{end}) or
					    $logger->error("Error searching by coord: $DBI::errstr");


	if(my ($id) = $self->{find_by_coord}->fetchrow_array) {
	    $logger->trace("Found gene $id");
	    return $id;
	}
    } elsif($gene_header->{ref}) {
	$logger->trace("Looking up by ref: $accnum, " . $gene_header->{ref});
	$self->{find_by_ref}->execute($accnum,
				      $gene_header->{ref}) or
					  $logger->error("Error searching by ref: $DBI::errstr");
					  

	if(my ($id) = $self->{find_by_ref}->fetchrow_array) {
	    return $id;
	}
    }

    return undef;
}

# Find all the virulence genes and write them out of a fasta file

sub make_vir_fasta {
    my $self = shift;
    my $accnum = shift;

    my $dbh = Islandviewer::DBISingleton->dbh;

    # Get a GenomeUtils object so we can do lookups
    my $genome_utils = Islandviewer::GenomeUtils->new({microbedb_ver => $self->{microbedb_ver} });

    # Find ourself and then filter down our name for the comparison step
    my $genome_obj = $genome_utils->fetch_genome($accnum);
    unless($genome_obj->genome_status() eq 'READY') {
	$logger->error("Failed in fetching genome object for $accnum, this shouldn't happen!");
	return undef;
    }
    
    my $find_vir_genes = $dbh->prepare("SELECT virulence.protein_accnum, virulence.external_id, virulence.source, virulence.flag, virulence.pmid, Genes.start, Genes.end, Genes.strand  from virulence, Genes where Genes.ext_id = ? AND Genes.name = virulence.protein_accnum AND virulence.type='virulence' AND virulence.source!='BLAST' and virulence.source!='PAG'");

    $find_vir_genes->execute($accnum);

    # Make a temporary file and a fasta file for the virulence sequences
    my $query_file = $self->_make_tempfile();
    my $out = Bio::SeqIO->new(-file => ">$query_file" ,
				  -format => 'Fasta');
    $logger->trace("Making temporary fasta file $query_file");
	
    while(my @row = $find_vir_genes->fetchrow_array) {
	# Fetch the sequence from the fna file
	my $seq = $genome_utils->fetch_protein_seq($genome_obj, $row[7], $row[5], $row[6]);

	my @display_id = ("ref|" . $row[0] . "|ext_id|" . $row[1] . "|source|" . $row[2]);
	if($row[3]) {
	    push @display_id, "flag|" . $row[3];
	}
	if($row[4]) {
	    push @display_id, "pmid|" . $row[4];
	}
	my $display = join '|', @display_id;

        $logger->trace("Writing sequence for protein: $display");
	my $pseq = Bio::PrimarySeq->new(
	    -display_id => $display, 
	    -seq => $seq,
	    -alphabet => 'protein');

	$out->write_seq($pseq);
	
    }

    return $query_file;
}

# Little helper to shorten the genome name of the pieces
# we don't want

sub filter_name {
    my $self = shift;
    my $name = shift;

    $name =~ s/ chromosome, complete genome.//;
    $name =~ s/, complete sequence.//;
    $name =~ s/, complete genome.//;
    $name =~ s/plasmid.+//;

    return $name;

}

# Filter to see if the name matches between the two
# genomes using our fragile filtering criteria

sub check_names_match {
    my $self = shift;
    my $def = shift;
    my $sub_def = shift;

    $logger->trace("[$def] vs [$sub_def]");

    my $okay = 0;

    if ($sub_def =~ /^Escherichia coli/){
	if ($sub_def =~/Escherichia coli str. (.+) substr.+/){
	    my $str = $1;
	    if ($def =~ /Escherichia coli str. (.+) substr.+/){
		if ($1 eq $str){
		    $okay = 1;
		}
	    }
	}
	elsif ($sub_def =~ /Escherichia coli (.+)str. (.+)/){
	    my $str = $1;
	    if ($def =~ /Escherichia coli (.+)str. (.+)/){
		if ($1 eq $str){
		    $okay = 1;
		}
	    }
	}
	elsif ($sub_def =~ /Escherichia coli (.+)/){
	    my $str = $1;
	    if ($def =~ /Escherichia coli (\.+)/){
		if ($1 eq $str){
		    $okay = 1;
		}
	    }
	}
    }
    elsif ($sub_def =~ /(.+)subsp\. (.+)serovar (\w+).+/){
	my $genussp = $1;
	my $subsp = $2;
	my $serovar = $3;
	if ($def =~ /(.+)subsp\. (.+)serovar (\w+).+/){
	    if ($1 eq $genussp && $2 eq $subsp && $3 eq $serovar){
		$okay = 1;
	    }
	}
    }	
    elsif ($sub_def =~ /(.+)subsp\. (.+) .+/){
	my $genussp = $1;
	my $subsp = $2;
	if ($def =~ /(.+)subsp\. (.+) .+/){
	    if ($1 eq $genussp && $2 eq $subsp){
		$okay = 1;
	    }
	}
			
    }			
    elsif($sub_def =~ /(.+)pv\. (.+) .+/){
	my $genussp = $1;
	my $pavar = $2;
	if ($def =~ /(.+)pv\. (.+) .+/){
	    if ($1 eq $genussp && $2 eq $pavar){
		$okay = 1;
	    }
	}
	
    }
    elsif ($sub_def =~ /(\w+) (\w+) .+/){
	my $genus = $1;
	my $species = $2;
	if ($def =~ /(\w+) (\w+) .+/){
	    if ($1 eq $genus && $2 eq $species){
		$okay = 1;
	    }
	}
    }

    return $okay;

}

# Make a temp file in our work directory and return the name

sub _make_tempfile {
    my $self = shift;

    # Let's put the file in our workdir
    my $tmp_file = mktemp(File::Spec->catpath(undef, $self->{workdir}, "blasttmpXXXXXXXXXX"));
    
    # And touch it to make sure it gets made
    `touch $tmp_file`;

    return $tmp_file;
}

1;
