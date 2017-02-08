#!/usr/bin/env perl

use Getopt::Long;
use DBI;


my $HOST     = "";
my $DATABASE = "";
my $USER     = "";
my $PASSWORD = "";

# retrieve options from command line
my $ERROR = GetOptions(
    "host=s"     => \$HOST,
    "database=s" => \$DATABASE,
    "user=s"     => \$USER,
    "password=s" => \$PASSWORD,
);
if ($HOST eq "") {
    die "\n\nPlease specify the \$HOST --host\n";
}
if ($DATABASE eq "") {
    die "\n\nPlease specify the \$DATABASE --database\n";
}
if ($USER eq "") {
    die "\n\nPlease specify the \$USER --user\n";
}
if ($PASSWORD eq "") {
    die "\n\nPlease specify the \$PASSWORD --password\n";
}



my $dsn    = "dbi:mysql:database=$DATABASE;host=$HOST";
my $dbh    = DBI->connect($dsn, $USER, $PASSWORD) or die DBI->errstr;
if ($dbh) {
    print "Successfully connected to *** $DATABASE ***\n";
}

