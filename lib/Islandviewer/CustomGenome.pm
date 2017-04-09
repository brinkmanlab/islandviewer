=head1 NAME

    Islandviewer::CustomGenome

=head1 DESCRIPTION

    Object for holding and managing a custom genome

=head1 SYNOPSIS

    use Islandviewer::CustomGenome;


=head1 AUTHOR

    Matthew Laird
    Brinkman Laboratory
    Simon Fraser University
    Email: lairdm@sfu.ca

=head1 LAST MAINTAINED

    February 4, 2015

=cut

package Islandviewer::CustomGenome;

use strict;
use warnings;
use Log::Log4perl;
use Data::Dumper;
use Moose;
use Moose::Util::TypeConstraints;
use Carp qw( confess );
use MIME::Base64::URLSafe;
use File::Temp qw/ :mktemp /;
use File::Basename;
use File::Copy;
use JSON;
use File::Spec;

use Islandviewer::DBISingleton;
use Islandviewer::Constants qw(:DEFAULT $STATUS_MAP $REV_STATUS_MAP $ATYPE_MAP);

has cid => (
    is     => 'rw',
    isa    => 'Int',
    default => 0
);

has name => (
    is     => 'rw',
    isa    => 'Str'
);

has owner_id => (
    is     => 'rw',
    isa    => 'Int',
    default => 1
);

has cds_num => (
    is     => 'rw',
    isa    => 'Int'
);

has rep_size => (
    is     => 'rw',
    isa    => 'Int'
);

subtype 'pathStr'
  => as 'Str'
;

coerce 'pathStr'
  => from 'Str'
    => via { return Islandviewer::Config->expand_directory( $_ ) }
;

has filename => (
    is     => 'rw',
    isa    => 'pathStr',
    coerce => 1
);

subtype 'ArrayRefofStr'
  => as 'ArrayRef[Str]'
;

coerce 'ArrayRefofStr'
  => from 'Str'
    => via { [ split / / ] }
#    => via { my @arr = split / /, $_; print Dumper \@arr; return \@arr }
#    => via { split(' ', $_) }
#  => from 'ArrayRef[Str]'
#    => via { $_ }
#    => via { [ map { $_ } @$_ ] }
;

#subtype 'My::ArrayRef' => as 'ArrayRef';

#    coerce 'My::ArrayRef'
#        => from 'Str'
#        => via { [ split / / ] };

#    coerce 'My::ArrayRef'
#        => from 'ArrayRef[Str]'
#        => via { print Dumper $_; return [ $_ ] };

has formats => (
    traits  => ['Array'],
    is      => 'rw',
    isa     => 'ArrayRefofStr',
#    isa     => 'My::ArrayRef[Ref]',
    coerce  => 1,
    default => sub { [] },
);

has contigs => (
    is     => 'rw',
    isa    => 'Int'
);

has genome_status => (
    is      => 'rw',
    isa     => enum([qw(NEW UNCONFIRMED MISSINGSEQ MISSINGCDS VALID READY INVALID)]),
    default => 'NEW',
    trigger => \&update_genome,
);

my $logger; my $cfg;

sub BUILD {
    my $self = shift;
    my $args = shift;

    $logger = Log::Log4perl->get_logger;

    $cfg = Islandviewer::Config->config;

    if($args->{load}) {
	$self->loadGenome($args->{load});
    } else {
	$self->genome_status('NEW');
    }

    if(defined($args->{owner_id})) {
	$logger->debug("Setting owner_id to " . $args->{owner_id});
	$self->owner_id($args->{owner_id});
    }

    if($args->{microbedb_ver}) {
	$self->{microbedb_ver} = $args->{microbedb_ver};
    }

}

sub loadGenome {
    my $self = shift;
    my $cid = shift;

    my $dbh = Islandviewer::DBISingleton->dbh;

    my $sqlstmt = qq{SELECT name, owner_id, cds_num, rep_size, filename, formats, contigs, genome_status FROM CustomGenome WHERE cid = ?};
    my $fetch_cg = $dbh->prepare($sqlstmt) or die "Error preparing statement: $sqlstmt: $DBI::errstr";

    $logger->debug("Fetching custom genome [$cid]");
    $fetch_cg->execute($cid);

    if(my $row = $fetch_cg->fetchrow_hashref) {
	# Load the pieces
	for my $k (keys %$row) {
	    if(defined($row->{$k})) {
		if($k eq 'filename') {
		    # Stupid hack because the coerce in the definiton won't fucking work
		    $self->$k( Islandviewer::Config->expand_directory( $row->{$k} ) );
		    next;
		}
		$self->$k($row->{$k});
	    }
	}
    }

#    print $self->dump() . "\n";

    $self->cid($cid);
}

sub validate {
    my $self = shift;
    my $args = shift;

    $logger->trace("Args given: " . join(',', keys %{$args}));

    # If we're brand new, there better be a genome file (genbank or embl)
    # for us to use. Save it to disk, save ourselves to the db,
    # and we become unconfirmed
    if($self->genome_status eq 'NEW') {
	unless($args->{genome_data} && $args->{genome_format}) {
	    $logger->logdie("No genome data given, this is a failure [NOGENOMEDATA]");
	}

	$self->filename( $self->write_genome($args->{genome_data}, $args->{genome_format}) );
	$self->name( $args->{genome_name} ? $args->{genome_name} : 'User Genome' );
	$self->genome_status('UNCONFIRMED');
	$self->save_genome($args);
	$logger->trace("NEW genome set to UNCONFIRMED: " . $self->filename . ', ' . $args->{genome_format});
    }

    # Alright, this will catch both a new genome that's transitioned
    # in to an unconfirmed, and a returning call with updated fna
    # file info
    if($self->genome_status eq 'UNCONFIRMED' || $self->genome_status eq 'MISSINGSEQ' || $self->genome_status eq 'VALID') {
	$logger->trace("Processing " . $self->genome_status . ' cid: ' . $self->cid . ' type genome: ' . $self->filename);
	if($args->{fna_data}) {
	    $logger->trace("Found fna_data for " . $self->cid);
	    $self->write_genome($args->{fna_data}, 'fna');
	} elsif($self->genome_status eq 'MISSINGSEQ') {
	    # We were in MISSINGSEQ and didn't find an fna_data?
	    # Uh-oh, that's not good.

	    # Signal that we need an fna file
	    $logger->logdie("We don't have sequence information and were't given an fna, file " . $self->filename . " [NOSEQNOFNA]");
	}

	# We should be ready to try to validate...
	my $genome_obj = Islandviewer::GenomeUtils->new(
	{ workdir => $cfg->{workdir} });

	# What happens when we check the file...
	my $contigs;
	eval{ 
	    $logger->trace("Reading and checking genome " . $self->cid . ', file ' . $self->filename);
	    $contigs = $genome_obj->read_and_check($self);
#	    $contigs = $genome_obj->read_and_check($self->filename);
	};
	if($@) {
	    $logger->trace("Msg: $@");
	    if($@ =~ /FILEFORMATERROR/) {
		$self->genome_status('INVALID');
#		$self->update_genome();
		$logger->logdie("Invalid file format for file " . $self->filename ." [FILEFORMATERROR]");
	    } elsif($@ =~ /NOSEQFNA/) {
		$self->genome_status('INVALID');
#		$self->update_genome();
		$logger->logdie("Missing sequenceinformation for file " . $self->filename . ", FNA file was found [NOSEQFNA]");
	    } elsif($@ =~ /NOSEQNOFNA/) {
		$self->genome_status('MISSINGSEQ');
#		$self->update_genome();
		$logger->logdie("Missing sequence information for file " . $self->filename . ", FNA file was not found [NOSEQNOFNA]");
	    } elsif($@ =~ /NOCDSRECORDS/) {
		$self->genome_status('INVALID');
#		$self->update_genome();
		$logger->logdie("Missing cds records for file " . $self->filename . " [NOCDSRECORDS]");		
	    }
	}

	# Some sanity checking in case they upload
	# a genome with zero contigs...
	unless($contigs > 0) {
	    $self->genome_status('INVALID');
#	    $self->update_genome();
	    $logger->logdie("Invalid file format for file, no contigs " . $self->filename ." [FILEFORMATERROR]");
	}

	$self->contigs($contigs);
	$self->genome_status('VALID');
#	$self->update_genome();

	# We're valid, but are we ready to go, do we have an reference
	# genome if there's more than one contig?
	if($contigs > 1 && ! $args->{ref_accnum}) {
	    $logger->logdie("No reference sequence for " . $self->filename . ", please supply one or try automated selection [NOREFSEQUENCE]");
	}

	# We should be ready now!
	$self->genome_status('READY');

	return $contigs;

    }

}

sub scan_genome {
    my $self = shift;

    # We only allow scanning of the genome if we're in a state of READY
    unless($self->genome_status eq 'READY') {
	$logger->trace("Genome " . $self->cid . " not READY, bailing");
	return 0;
    }

    # Make a GenomeUtils objects to do the work
    my $genome_obj = Islandviewer::GenomeUtils->new(
	{ workdir => $cfg->{workdir} });

    # Find the file types, set the second parameter to true
    # to return an array instead of a string.
    my @formats = $genome_obj->find_file_types($self->filename, 1);
    $logger->trace("Found formats for " . $self->filename . ": [@formats]");
    $self->formats( \@formats );
    $logger->trace("For " . $self->cid . " found file formats: " . join(' ' , sort @{$self->formats()}) );

    # Next we need to scan the file to find CDS numbers and total length
    my $stats = $genome_obj->genome_stats($self->filename);

    foreach my $key (keys $stats) {
	$logger->trace("For file " . $self->filename . " found $key: " . $stats->{$key});
        if($key eq 'name') {
            $self->$key($stats->{$key}) if( $self->$key eq 'Custom Genome');
        } else {
            $self->$key($stats->{$key});
        }
    }

    # And save the updates...
    $self->update_genome();

    return 1;
}

sub write_genome {
    my $self = shift;
    my $genome_data = shift;
    my $genome_format = shift;

    my $decoded_genome_data = urlsafe_b64decode($genome_data);

    # Write out the genome file
    my $base_tmp_file;

    if($self->filename) {
	$logger->trace("Found an existing filename: " . $self->filename);
	$base_tmp_file = $self->filename;
    } else {
	$base_tmp_file = mktemp($cfg->{tmp_genomes} . "/custom_XXXXXXXXX");
#    push @tmpfiles, $tmp_file;
    }

    my $tmp_file = $base_tmp_file . ".$genome_format";
    $logger->trace("Using filename: $tmp_file");

    open(TMP_GENOME, ">$tmp_file") or 
	$logger->logdie("Error, can't create tmpfile $tmp_file: $@");

    print TMP_GENOME $decoded_genome_data;

    close TMP_GENOME;

    return $base_tmp_file
}

sub save_genome {
    my $self = shift;
    my $args = shift;

    my $dbh = Islandviewer::DBISingleton->dbh;

    my ($params, $values) = $self->build_params();

    my $sqlstmt = qq{INSERT INTO CustomGenome (} . join(',', @$params) . ") VALUES (" . join( ',', ('?') x @$values ) . ')';

    my $insert_cg = $dbh->prepare($sqlstmt) or die "Error preparing statement: $sqlstmt: $DBI::errstr";
    
    $insert_cg->execute(@$values);

    my $cid = $dbh->last_insert_id( undef, undef, undef, undef );

    $self->cid($cid);

    # Let's blindly try to insert, if we fail, we really don't care
    # since we're not sure if we'll ever use this table.
    eval {
	$dbh->do('INSERT INTO UploadGenome (filename, ip_addr, genome_name, email, cid) VALUES (?, ?, ?, ?, ?)', {}, $self->filename(), ($args->{ip_addr} ? $args->{ip_addr} : '0.0.0.0'), $self->name, ($args->{email} ? $args->{email} : ''), $self->cid());
    };
    if($@) {
	$logger->error("Error inserting in to UploadGenome: $@");
    }

    # Now we need to move things in to place, so we're nice
    # and tidy with our file organization
    $logger->trace("Moving genome " . $self->cid . ' in to place at ' . $cfg->{custom_genomes} . "/" . $self->cid);

    unless(mkdir(File::Spec->catpath(undef, $cfg->{custom_genomes}, $self->cid))) {
	$logger->error("Error, can't make custom genome directory $cfg->{custom_genomes}/" . $self->cid . ": $!");
	return 0;
    }
    unless($self->move_and_update(File::Spec->catpath(undef, $cfg->{custom_genomes}, $self->cid))) {
	$logger->error("Error, can't move files to custom directory for cid " . $self->cid);
    }

    return $cid;
}

sub move_and_update {
    my $self = shift;
    my $new_path = shift;

    $logger->info("Trying to move genome " . $self->cid . " to new location at $new_path");

    # First let's ensure this is a directory
    unless( -d $new_path ) {
	$logger->error("Error, $new_path doesn't seem to be a directory");
	return 0;
    }

    my($filename, $directory, $suffix) = 
	fileparse($self->filename);

    # Move the files over to the new location
#    my @old_files = glob ($self->{base_filename} . '*');
    foreach my $f (glob ($self->filename . '*')) {
	$logger->trace("Moving: " . $f . ' to ' . $new_path);
	move($f, $new_path);
    }

    # Now update the base name in the database
    my $newfile = "$new_path/$filename";
    $newfile =~ s/\/\//\//g;
    $self->filename( $newfile );

    $self->update_genome();
    return 1;

}

sub update_genome {
    my $self = shift;

#    print $self->dump() . "\n";

    # If we haven't saved the genome already we can't do an update
    return unless($self->cid);

    my $dbh = Islandviewer::DBISingleton->dbh;

    my ($params, $values) = $self->build_params();

    my $sqlstmt = qq{REPLACE INTO CustomGenome (cid, } . join(',', @$params) . ") VALUES (?," . join( ',', ('?') x @$values ) . ')';

    my $insert_cg = $dbh->prepare($sqlstmt) or die "Error preparing statement: $sqlstmt: $DBI::errstr";

    unshift @$values, $self->cid;

    my $res = $insert_cg->execute(@$values);

    $logger->trace("Updated " . $self->cid . " results: $res");

}

sub build_params {
    my $self = shift;

    my @params; my @values;

    if($self->name) {
	push @params, 'name';
	push @values, $self->name;
    }

    if($self->owner_id) {
	push @params, 'owner_id';
	push @values, $self->owner_id;
    }

    if($self->cds_num) {
	push @params, 'cds_num';
	push @values, $self->cds_num;
    }

    if($self->rep_size) {
	push @params, 'rep_size';
	push @values, $self->rep_size;
    }

    if($self->filename) {
	push @params, 'filename';
	my $path = Islandviewer::Config->shorten_directory($self->filename);

	push @values, $path;
    }

    if($self->formats) {
	push @params, 'formats';
	my $formats = $self->formats();
#	print Dumper $formats;
#	print join(' ', @$formats);
	push @values, join( ' ', sort @$formats);
#	push @values, join( ' ', sort $self->formats());
    }

    if($self->contigs) {
	push @params, 'contigs';
	push @values, $self->contigs;
    }

    if($self->genome_status) {
	push @params, 'genome_status';
	push @values, $self->genome_status;
    }

#    print Dumper @values;
    return (\@params, \@values);
}

sub atype {
    my $self = shift;

    return $ATYPE_MAP->{custom};
}

sub dump {
    my $self = shift;

    my $json_data;

    $json_data->{cid} = $self->cid;
    $json_data->{name} = $self->name;
    $json_data->{owner_id} = $self->owner_id;
    $json_data->{cds_num} = $self->cds_num;
    $json_data->{rep_size} = $self->rep_size;
    $json_data->{filename} = $self->filename;
    $json_data->{formats} = $self->formats;
    $json_data->{contigs} = $self->contigs;
    $json_data->{genome_status} = $self->genome_status;

    my $json = to_json($json_data, { pretty => 1 });

    return $json;
}

1;
