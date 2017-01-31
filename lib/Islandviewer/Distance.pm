=head1 NAME

    Islandviewer::Distance

=head1 DESCRIPTION

    Object to calculate distance between replicons using MASH, depends on
    MicrobeDBv2

=head1 SYNOPSIS

    use Islandviewer::Distance;

    $dist = Islandviewer::Distance->new({workdir => $workdir);
    $dist->calculate_all(version => 73, custom_replicon => $repHash);

    # Where $repHash->{$cid} = $filename # with .fna extension

    $distance->add_replicon(cid => 2, version => 73);

=head1 AUTHOR

    Claire Bertelli
    Email: claire.bertelli@sfu.ca
    and
    Matthew Laird
    Email: lairdm@sfu.ca
    Brinkman Laboratory
    Simon Fraser University


=head1 LAST MAINTAINED

    Jan 27, 2017

=cut


package Islandviewer::Distance;

use strict;
use Moose;
use File::Basename;
use File::Spec;
use File::Copy;
use Log::Log4perl qw(get_logger :nowarn);
use Data::Dumper;
use File::Temp qw/ :mktemp /;
use File::Path qw(rmtree);
use IO::File;

use MicrobedbV2::Singleton;

use Islandviewer::Schema;
use Islandviewer::GenomeUtils;

my $cfg; my $logger; my $cfg_file;

my $module_name = 'Distance';

sub BUILD {
    my $self = shift;
    my $args = shift;

    $cfg = Islandviewer::Config->config;
    $cfg_file = File::Spec->rel2abs(Islandviewer::Config->config_file);

    $logger = Log::Log4perl->get_logger;

    $self->{microbedb_ver} = (defined($args->{microbedb_ver}) ?
			      $args->{microbedb_ver} : undef );

    die "Error, work dir not specified:  $args->{workdir}"
		unless( -d $args->{workdir} );
    $self->{workdir} = $args->{workdir};

	# check that we have the mash command and file in the config
	die "Error, mash cmd and sketch not specified:  $cfg->{mash_cmd}"
		unless( $cfg->{mash_cmd} & $cfg->{mash_sketch} );
	$self->{mash_cmd} = $cfg->{mash_cmd};
	$self->{mash_sketch} = $cfg->{mash_sketch};

    $self->{schema} = Islandviewer::Schema->connect($cfg->{dsn},
					       $cfg->{dbuser},
					       $cfg->{dbpass})
	or die "Error, can't connect to Islandviewer via DBIx";

	$self->{dist_table} = $cfg->{dist_table};

    # Save the args for later
    $self->{args} = $args;

    # Vocalize a little
    $logger->info("Initializing Islandviewer::Distance");
    $logger->info("Using workdir " .  $self->{workdir});

}

# Entry point for custom genomes
sub run {
    my $self = shift;
    my $accnum = shift;
    my $callback = shift;

    $self->{accnum} = $accnum;

	(my $version, my $ratiosuccess) =  $self->add_replicon(cid => $accnum);

	$self->{args}->{ratiosuccess} = $ratiosuccess;
	if($callback) {
	    $callback->update_args($self->{args});
	}

	# Declare victory or failure.... 
	# let's do some basic sanity testing....
    if ($ratiosuccess != 1) {
		$logger->error("Error, Mash distance step failed");
		$callback->set_status("ERROR");
		return 0;
	}

	$logger->info("Mash distance has been successfully completed");
    return 1;
}

# Main function that will calculate all the distances using MASH. Entrypoint for updates distance calculation.
sub calculate_all {
    my $self = shift;
    my (%args) = @_;

    my $version = ($args{version} ? $args{version} : undef );
    my $custom_rep = ($args{custom_replicon} ?
		      $args{custom_replicon} : undef );

    my $replicon;

    # Check the version we're given
    $version = $self->set_version($version);
    $logger->debug("Using MicrobeDB version $version");

    die "Error, not a valid version"
	unless($version);

    my $microbedb = MicrobedbV2::Singleton->fetch_schema;

    my $rep_results = $microbedb->resultset('Replicon')->search( {
            rep_type => 'chromosome',
            version_id => $version
        }
    );

    # Loop through the results and store them away
    while( my $curr_rep_obj = $rep_results->next() ) {
	my $rep_accnum = $curr_rep_obj->rep_accnum . '.' . $curr_rep_obj->rep_version;
	my $filename = $curr_rep_obj->get_filename('fna');

	$replicon->{$rep_accnum} = $filename
	    if($filename && $rep_accnum);
    }

    $logger->debug("Found " . scalar(keys %{$replicon}) . " replicons from microbedb");

    # Once we have all the possible replicons let's define the set of genomes we need to compare
    # if we're running a custom replicon set, use that for the
    # first sets in the pairs comparison
    # and for future expansion we'll want to allow custom replicons to
    # run against each other....
    my $runpairs; my $custom_vs_custom = 0; my $allvsall = 0; my $nbpairstorun;
    # If there is a custom_rep, we are not doing a distance calculation for the update
    if($custom_rep) {
        # If there's more than one custom replicon, we're
        # running them against themselves
        if(scalar(keys %{$custom_rep}) > 1) {
            $logger->info("Running custom vs custom");
            $runpairs = $custom_rep;
            $custom_vs_custom = 1;
            $nbpairstorun = scalar(keys %{$custom_rep})*scalar(keys %{$custom_rep});
        } else {
            # If the custom genome is part of the update, we dont need to run the distance step because it was done before
            $logger->info("Checking if this is an update or a custom genome");
            my $isupdate = $self->check_if_update($custom_rep);
            if ($isupdate == 1) {
                $logger->info("This is an update - no need to calculate distances again");
                return ($version, 1);
            }
            # Otherwise we're just running a custom genome against all the microbedb genomes
            $logger->info("Running single custom genome vs microbedbv2");
            $runpairs = $custom_rep;
            $nbpairstorun = scalar(keys %{$custom_rep})*scalar(keys %{$replicon});
        }
    } else {
	# We are doing a distance update, comparing microbedb everything vs everything...
		$allvsall = 1;
		$runpairs = $replicon;
		$nbpairstorun = scalar(keys %{$replicon})*scalar(keys %{$replicon});
    }

    $logger->info("We have " . scalar(keys %{$runpairs}) . " replicons to run");

    # Remember the number of pairs we're wanting to run
    $self->{runnum} = scalar(keys %{$runpairs});

    # Why do all the rest if we have nothing to run
    if($self->{runnum} == 0) {
	$logger->info("Nothing to run, goodbye.");
	return ($version, 0);
    }

	# We need a directory to store temporary genome files
	my $tmp_dir = $self->_make_tempdir();

	# define the standard mash file
	my $mash_file = $self->{mash_sketch};

	# We need to distinguish three cases:
	# - we are doing an update and we need to replace the existing mash file.
	# - we want to compare custom genomes between them and need a temporary mash sketch file
	# - we want to compare a custom genome to the standard microbedbv2 genomes and we can use the existing mash sketch file
	if ($allvsall==1) {
		$logger->info("Building sketch from scratch");
		my $tmp_mash_file = $self->build_sketch($runpairs, $tmp_dir);

		# Copy the new reference sketch file to the standard location and test for success
		my $cp_cmd = "cp " . $tmp_mash_file . " " . $mash_file;
		system($cp_cmd);
		die "Error, copying mash sketch file from $mash_file was not successfull"
			unless( -f $mash_file );
		$logger->info("The reference sketch was successfully built");
	} elsif ($custom_vs_custom==1) {
		# we have to build a new sketch for comparison between custom genomes
		$logger->info("Need to add genomes to the sketch");
		$mash_file = $self->build_sketch($runpairs, $tmp_dir);
		# the combination of sketches with the reference sketch is not necessary if all genomes have already been processed
		# through the pipeline, so commenting out this line for the moment
		# $mash_file = $self->combine_sketch($mash_file);
	} else {
		# we need to have a pre-built sketch file
		die "Error, mash sketch not found:  $cfg->{mash_sketch}"
			unless( -f $cfg->{mash_sketch});

		$logger->info("Simply comparing a custom genome to the sketch");
		$self->prepare_files($runpairs, $tmp_dir);
	}

	$logger->info("Comparing genomes to sketch");
	my $mash_results = $self->submit_mash($tmp_dir, $mash_file);
	$logger->info("Parsing mash distance");
	(my $distance_file, my $nbpairsobtained) = $self->parse_mash($mash_results);

	my $ratiosuccess = $nbpairsobtained/$nbpairstorun;
	$logger->info("Our rate of success for distance was $ratiosuccess");
	if ($ratiosuccess != 1) {
		$logger->error("The distance calculation failed");
		die;
	}

	# now if we are building a new reference dataset from MicrobeDBv2, we need to remove all the old MicrobeDBv2 comparison
    if ($allvsall==1) {
		$self->remove_dist();
	}

	# Load the results in the database
	$self->load_dist($distance_file);

	# remove all temp data
	$logger->trace("Cleaning up temp dir for Mash");
	$self -> _remove_tmpdir($tmp_dir);

    # Return the version we used just for ease of use
    return ($version, $ratiosuccess);
}

# Function to check if a custom replicon is part of an islandviewer update
sub check_if_update {
	my $self = shift;
	my $custom_rep = shift;

	# Fetch the DBH
	my $dbh = Islandviewer::DBISingleton->dbh;

	# Check if the replicon id is already in the Distance table, it has been processed already
	my $existingdistance;
	foreach my $repid (keys %{$custom_rep}) {
		print $repid;
		my $sth = $dbh->prepare("SELECT COUNT(*) FROM $self->{dist_table} WHERE rep_accnum1=$repid") or
			$logger->logdie("Error selecting lines" . $DBI::errstr);
		$sth->execute();
		$existingdistance = $sth->fetchrow_array;
		$logger->info("There are already $existingdistance distance values for $repid in the database");
	}

	# By default this is a custom genome, but if the repid is already in the database, the distance step has ran already
	# and this is an islandviewer update
	my $isupdate = 0;
	if ($existingdistance != 0){
		$isupdate = 1;
	}
    return ($isupdate);
}

# Function to build a mash sketch to compare other genomes with
sub build_sketch {
	my $self = shift;
    my $genome_set = shift;
    my $tmp_dir = shift;

	$self->prepare_files($genome_set, $tmp_dir);

	# Now build the command and submit mash
	$logger->info("Now building the sketch");
	my $tmp_mash_file = $tmp_dir . "/mash_sketch_10000";
	my $cmd = $self->{mash_cmd} . " sketch -s 10000 -o " . $tmp_mash_file . " " . $tmp_dir . "/" . "*.fna";

	unless ( open( COMMAND, "$cmd |" ) ) {
		$logger->logdie("Cannot run $cmd");
	}

	#Waits until the system call is done before saving to the array
	my @stdout = <COMMAND>;
	close(COMMAND);

	# We need to update the name of the mash file, to add the extension used by mash
	$tmp_mash_file = $tmp_mash_file . ".msh";
	# Check that the temporary mash sketch file exists
	die "Error, mash sketch file was not created:  $tmp_mash_file"
		unless( -f $tmp_mash_file );

	$logger->info("Finished building the sketch");

	return $tmp_mash_file;
}

# function that copies the fna files to sketch/compare to the sketch to the temp directory and changes their name
# to the entire accession.version number for ease of further processing
sub prepare_files {
	my $self = shift;
	my $genome_set = shift;
	my $tmp_dir = shift;

	# Iterate through the genome_set, cp the file to the tmp
	$logger->info("Copying files to tmp");

	foreach my $acc (keys %{$genome_set}) {
		my $genome_file = $genome_set->{$acc};
		my $output_file = $tmp_dir . "/" . $acc . ".fna";
		my $cp_cmd = "cp " . $genome_file . " " . $output_file ;
		system($cp_cmd);
		die "Error, file was not copied:  $output_file"
			unless( -f $output_file );
	}
	$logger->info("Finished copying files to tmp");
	return 1;
}

# function that combines the reference sketch and a temporary sketch, not used for now
sub combine_sketch {
	my $self = shift;
	my $mash_file = shift;

	my $combined_mash_file = $mash_file . "_combined";

	my $cmd = $self->{mash_cmd} . " paste " . $combined_mash_file . " " . $self->{mash_sketch} . " " . $mash_file;

	unless ( open( COMMAND, "$cmd |" ) ) {
		$logger->logdie("Cannot run $cmd");
	}

	#Waits until the system call is done before saving to the array
	my @stdout = <COMMAND>;
	close(COMMAND);

	return $combined_mash_file;
}

# function that does the comparison of genomes in the temp directory with a mash sketch file (.msh)
sub submit_mash {
	my $self = shift;
	my $tmp_dir = shift;
	my $mash_file = shift;

	# We need a temporary file to store the results
	my $mash_results = $tmp_dir . "/mash_distance.txt";
	# Now build the command and submit it
	my $cmd = $self->{mash_cmd} . " dist " . $mash_file . " " . $tmp_dir . "/*.fna > " . $mash_results;

	unless ( open( COMMAND, "$cmd |" ) ) {
		$logger->logdie("Cannot run $cmd");
	}

	#Waits until the system call is done before saving to the array
	my @stdout = <COMMAND>;
	close(COMMAND);

	# Check that the result file is there
	die "Error, mash results file was not created:  $mash_results"
		unless( -f $mash_results );

	return $mash_results;
}

# function that reads the mash result file and turns it into a three column table in the format of the Distance table
sub parse_mash {
	my $self = shift;
	my $mash_results = shift;
	my $formatted_results = $mash_results . "formatted.txt";

	# We're going to read the mash_results file line by line, make the necessary substitution and
	# write them to another file for upload
	open(my $file_handle, '<', $mash_results) or die "Could not open '$mash_results' for reading $!";
	open(my $outfile_handle, '>', $formatted_results) or die "Could not open '$formatted_results' for writing $!";
	my $i = 0;
	while ( my $line = <$file_handle> ) {
        # Note that we have to exchange the columns order so that remove_dist() does not remove custom genomes during updates
		$line =~ s/^.+\/([0-9A-Z\_\.]+)\.fna\t.+\/([0-9A-Z\_\.]+)\.fna\t(\d+\.?\d*[e\-0-9]*)\t.+/$2\t$1\t$3/g;
		# we will only keep distances between 0 and 0.3 to avoid storing unnecessary data
		my($dist) = $line =~ /[0-9A-Z\_\.]+\t[0-9A-Z\_\.]+\t(\d+.?\d*)/;
		$dist = $dist + 0;
		if ( $dist <= 0.3 ) {
			print $outfile_handle ($line);
		}
		$i++;
	}
	close($file_handle);
	close($outfile_handle);

	return ($formatted_results, $i);
}

# function to remove all existing distance between reference genomes in the previous update
# (normally previous version of MicrobeDBv2). Used only in case of an update.
sub remove_dist {
	my $self = shift;

	# Fetch the DBH
	my $dbh = Islandviewer::DBISingleton->dbh;

	$logger->info("Deleting previous MicrobeDBv2 distances");

	# Delete the previous MicrobeDBv2 distances
	$dbh->do("DELETE FROM $self->{dist_table} WHERE rep_accnum1 RLIKE '_' ") or
		$logger->logdie("Error deleting previous MicrobeDBv2 distances" . $DBI::errstr);

	$logger->info("Finished deleting previous MicrobeDBv2 distances");

	return 1;
}

# function to load the mash distance into the Distance table
sub load_dist {
	my $self = shift;
	my $results_file = shift;

	die "Error, can't access results file $results_file"
		unless( -f $results_file );

	$logger->info("Loading distances");

	# Fetch the DBH
	my $dbh = Islandviewer::DBISingleton->dbh;

	# Bulk load the results
	$dbh->do("LOAD DATA LOCAL INFILE '$results_file' REPLACE INTO TABLE $self->{dist_table} FIELDS TERMINATED BY '\t' (rep_accnum1, rep_accnum2, distance)") or
		$logger->logdie("Error loading $results_file:" . $DBI::errstr);

	$logger->info("Finished loading distances");
}

sub _make_tempdir {
	my $self = shift;

	# Let's put the file in our workdir
	my $tmp_dir = mkdtemp($self->{workdir} . "/mashtmpXXXXXXXXXX");

	# And touch it to make sure it gets made
	`touch $tmp_dir`;

	return $tmp_dir;
}

sub _remove_tmpdir {
	my $self = shift;
	my $tmpdir = shift;

	unless(rmtree $tmpdir) {
		$logger->error("Can't remove directory $tmpdir: $!");
	}
}

sub _remove_tmpdirarray {
	my $self = shift;
	my @tmpdir = @_;

	foreach my $dir (@tmpdir) {
		unless(rmtree $dir) {
			$logger->error("Can't remove directory $dir: $!");
		}
	}
}

# Entry the distance for a custom genome
sub add_replicon {
    my $self = shift;
    my (%args) = @_;

    my $cid = $args{cid};

    my $genome_obj = Islandviewer::GenomeUtils->new({microbedb_ver => $self->{microbedb_ver} });
    my($name,$filename,$format_str) = $genome_obj->lookup_genome($cid);

    my $formats = $genome_obj->parse_formats($format_str);

    # Do some checking on the file name and munge it for our needs
    unless($filename =~ /^\//) {
	# The file doesn't start with a /, its not an absolute path
	# fix it up, assume its under the custom_genomes folder
	$filename = $cfg->{custom_genomes} . "/$filename";
    }

    # Filenames are just saved as basenames, check if the fasta version exists
#    unless( -f "$filename.fna" ) {
    unless( $formats->{fna} ) {
	$logger->error("Error, can't find filename $filename.fna");
	return 0;
    }

    # We have a valid filename, lets toss it to calculate_all
    # pass along the specific microbedb version if we've
    # been given one
    my $custom_rep->{$cid} = "$filename.fna";
	my $version; my $ratiosuccess;
    if($args{version}) {
		($version, $ratiosuccess) = $self->calculate_all(custom_replicon => $custom_rep,
			     version => $args{version});
    } else {
		($version, $ratiosuccess) = $self->calculate_all(custom_replicon => $custom_rep);
    }

    return ($version, $ratiosuccess);

}

sub set_version {
    my $self = shift;
    my $v = shift;

    # Create a Versions object to look up the correct version
    my $microbedb = MicrobedbV2::Singleton->fetch_schema;

    # If we're not given a version, use the latest
    $v = $microbedb->latest() unless($v);

    # Is our version valid?
    return 0 unless($microbedb->fetch_version($v));

    return $v;
}

1;
