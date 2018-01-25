=head1 NAME

    Islandviewer::MicrobeDBGenome

=head1 DESCRIPTION

    Object for holding and managing a MicrobeDB Genome

=head1 SYNOPSIS

    use Islandviewer::MicrobeDBGenome;


=head1 AUTHOR

    Matthew Laird
    Brinkman Laboratory
    Simon Fraser University
    Email: lairdm@sfu.ca

=head1 LAST MAINTAINED

    February 8, 2015

=cut

package Islandviewer::MicrobeDBGenome;

use strict;
use warnings;
use Log::Log4perl;
use Data::Dumper;
use Moose;
use Moose::Util::TypeConstraints;
use Carp qw( confess );
use JSON;
use File::Spec;

use Islandviewer::DBISingleton;
use Islandviewer::Constants qw(:DEFAULT $STATUS_MAP $REV_STATUS_MAP $ATYPE_MAP);

use MicrobedbV2::Singleton;

has cid => (
    is     => 'rw',
    isa    => 'Str',
    default => 0
);

has name => (
    is     => 'rw',
    isa    => 'Str'
);

has owner_id => (
    is     => 'rw',
    isa    => 'Int',
    default => 0
);

has cds_num => (
    is     => 'rw',
    isa    => 'Int'
);

has rep_size => (
    is     => 'rw',
    isa    => 'Int'
);

has filename => (
    is     => 'rw',
    isa    => 'Str'
);

subtype 'MDBArrayRefofStr'
  => as 'ArrayRef[Str]'
;

coerce 'MDBArrayRefofStr'
  => from 'Str'
    => via { [ split / / ] }
  => from 'ArrayRef[Str]'
    => via { $_ }
  => from 'Undef'
    => via { '' };

has formats => (
    traits  => ['Array'],
    is      => 'rw',
    isa     => 'MDBArrayRefofStr',
    coerce  => 1,
    default => sub { [] },
);

has contigs => (
    is     => 'rw',
    isa    => 'Int',
    default => 1
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

    # Finding microbedb version

    # If we've been given a microbedb version AND its valid... 
    my $microbedb = MicrobedbV2::Singleton->fetch_schema;
    unless($args->{microbedb_ver} && $microbedb->fetch_version($args->{microbedb_ver})) {
	$args->{microbedb_ver} = $microbedb->current();
    }

    $self->{microbedb_ver} = $args->{microbedb_ver};

    if($args->{load}) {
	$self->loadGenome($args->{load});
    } else {
	$self->genome_status('NEW');
    }

}

sub loadGenome {
    my $self = shift;
    my $accnum = shift;

    # First we find the MicrobeDB record
    # We keep using the microbedb one (for now) because the file location
    # might change with new microbedb versions, so we need to always be able
    # to find it.
    $logger->trace("Searching for microbedb genome $accnum in version " .  $self->{microbedb_ver});
    my $microbedb = MicrobedbV2::Singleton->fetch_schema;

    my $rep_results = $microbedb->resultset('Replicon')->search( {
        rep_accnum => $accnum,
        version_id => $self->{microbedb_ver}
                                                              }
    )->first;
	
    # We found a result in microbedb
    if( defined($rep_results) ) {

	$self->name( $rep_results->definition );
	$self->cid( $accnum );
	$self->filename(File::Spec->catpath(undef, $rep_results->genomeproject->gpv_directory, $rep_results->file_name) );
	$self->cds_num( $rep_results->cds_num );
	$self->rep_size( $rep_results->rep_size );
	$self->formats( $rep_results->file_types ? $rep_results->file_types : '' );
	$self->contigs ( 1 );
	$self->genome_status( 'READY' );
    }

    # Make a GenomeUtils objects to do the work
    my $genome_utils = Islandviewer::GenomeUtils->new(
	{ workdir => $cfg->{workdir} });

    # Find the file types, set the second parameter to true
    # to return an array instead of a string.
    my $found_formats = $genome_utils->find_file_types($self->filename, 1);

    if( join(' ', sort @{$self->formats()}) ne join(' ', sort $found_formats) ) {
	$logger->warn("Warning, database says we have [" . join(' ', sort @{$self->formats()}) . "] doesn't match file system [" .  join(' ', sort $found_formats) . ']' );
	$self->formats( @$found_formats );
    }

    $logger->trace("For " . $self->cid . " found file formats: " . join(',' , sort @{$self->formats()}) );

    
}

# Do some basic checking, we'll probably never use this
# but we need to handler here in case anyone calls it.

sub validate {
    my $self = shift;
    my $args = shift;

    # We should be ready to try to validate...
    my $genome_obj = Islandviewer::GenomeUtils->new(
	{ workdir => $cfg->{workdir} });

    # What happens when we check the file...
    my $contigs;
    eval{ 
	$logger->trace("Reading and checking genome " . $self->cid . ', file ' . $self->filename);
	$contigs = $genome_obj->read_and_check($self);
#	$contigs = $genome_obj->read_and_check($self->filename);
    };
    if($@) {
	$logger->trace("Msg: $@");
	if($@ =~ /FILEFORMATERROR/) {
	    $self->genome_status('INVALID');
	    $logger->logdie("Invalid file format for file " . $self->filename ." [FILEFORMATERROR]");
	} elsif($@ =~ /NOSEQFNA/) {
	    $self->genome_status('INVALID');
	    $logger->logdie("Missing sequenceinformation for file " . $self->filename . ", FNA file was found [NOSEQFNA]");
	} elsif($@ =~ /NOSEQNOFNA/) {
	    $self->genome_status('MISSINGSEQ');
	    $logger->logdie("Missing sequence information for file " . $self->filename . ", FNA file was not found [NOSEQNOFNA]");
	} elsif($@ =~ /NOCDSRECORDS/) {
	    $self->genome_status('INVALID');
	    $logger->logdie("Missing cds records for file " . $self->filename . " [NOCDSRECORDS]");		
	}
    }

    # Some sanity checking in case they upload
    # a genome with zero contigs...
    unless($contigs > 0) {
	$self->genome_status('INVALID');
	$logger->logdie("Invalid file format for file, no contigs " . $self->filename ." [FILEFORMATERROR]");
    }

    $self->contigs($contigs);

    return $contigs;

}

sub scan_genome {
    my $self = shift;

    # We only allow scanning of the genome if we're in a state of READY
    unless($self->genome_status eq 'READY') {
	$logger->trace("Genome " . $self->cid . " not READY, bailing (this should never happen for a MicrobeDB type)");
	return 0;
    }

    # Make a GenomeUtils objects to do the work
    my $genome_obj = Islandviewer::GenomeUtils->new(
	{ workdir => $cfg->{workdir} });

    # Find the file types, set the second parameter to true
    # to return an array instead of a string.
    my @formats = $genome_obj->find_file_types($self->filename, 1);
    $logger->trace("Found formats for " . $self->filename . ": [@formats]");
    $self->formats( @formats );
    $logger->trace("For " . $self->cid . " found file formats: " . join(' ' , sort $self->formats()) );

    # Next we need to scan the file to find CDS numbers and total length
    my $stats = $genome_obj->genome_stats($self->filename);

    foreach my $key (keys $stats) {
	$logger->trace("For file " . $self->filename . " found $key: " . $stats->{$key});
	$self->$key($stats->{$key});
    }

    return 1;
}

sub write_genome {
    my $self = shift;

    $logger->warn("We don't write MicrobeDB records right now.");
}

sub save_genome {
    my $self = shift;

    $logger->warn("We don't save MicrobeDB records right now.");
}

sub update_genome {
    my $self = shift;

    $logger->warn("We don't update MicrobeDB records right now.");
}

sub move_and_update {
    my $self = shift;

    $logger->warn("We don't move MicrobeDB records right now.");
}

sub atype {
    my $self = shift;

    return $ATYPE_MAP->{microbedb};
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
