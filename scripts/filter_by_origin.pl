#!/ebi/services/tools/bin/perl -w
#
use strict;

my $MAX_ENTRIES = 10000; # Deafaut number of entries per output file
my $PREFIX; # prefix of the output files

main();

sub main {
	#
	#
	#

	my ($prefixes_hr, $file_name) = read_args();

	my $file_no = 1;
	open (IN, "<$file_name") or
		die("Can't open '$file_name' for reading.\n$!\n");

	my $ac_linetype = get_ac_linetype(\*IN);
  my $ac_linetype_len = length($ac_linetype);
	
	open (OUT, ">$PREFIX.$file_no") or
		die("Can't open '$PREFIX.$file_no' for writing.\n$!\n");
	
	my $entry_no = 0;
	my $first_lines = '';
	my $line;

	while ($line = <IN>) {
	
		$first_lines .= $line;

		if (substr($line,0 , $ac_linetype_len) eq $ac_linetype) {# ac number line

			$line =~ m/^$ac_linetype   ([A-Z]+)\d/;# grab the prefix
			my $prefix = $1;

			unless ($prefix) {# could not find an accno prefix

				die("ERROR: Malformed line\n$line\n.");
			}

			if ($prefixes_hr->{$prefix}) {# prefix is in the accepted set

				write_entry(\*OUT, \*IN, $first_lines);
				++$entry_no;

				if ($entry_no >= $MAX_ENTRIES) {# file full

					print STDERR ".";
					close(OUT);
					++$file_no;
					$entry_no = 0;
					open (OUT, ">$PREFIX.$file_no") or
						die("Can't open '$PREFIX.$file_no' for writing.\n$!\n");
				}
					
			} else {

				skip_entry(\*IN);
			}

			$first_lines = '';
		}
	}

	close(OUT);
	close(IN);

	print STDERR "\n";
}


sub write_entry {
	#
	#
	#
	my ($out_fh, $in_fh, $first_lines) = @_;

	print ($out_fh $first_lines);
	
	while (<$in_fh>) {

		print ($out_fh $_);
		
		last if (m|^//|);
	}
}


sub skip_entry {
	#
	#
	#
	my ($in_fh) = @_;

	while (<$in_fh>) {

		last if (m|^//|);
	}
}


sub get_ac_linetype {
	#
	#
	#
	my ($fh) = @_;

	my $line = <$fh>;
	seek($fh, 0, 0);

	if ($line =~ m/^ID/) {

		print(STDERR "File is in EBI format.\n");
		return "AC";

	} elsif ($line =~ m/^LOCUS/) {

		print(STDERR "File is in NCBI format.\n");
		return "ACCESSION";

	} else {

		die ("ERROR: unrecognised file format.\n$line\n\n");
	}
}



sub read_args {
	#
	#
	#
	
	use Getopt::Long;
  Getopt::Long::Configure ("bundling_override");

	use DBI;

	my ($progname) = $0 =~ m|([^/]+)$|;

	my $USAGE = <<USAGE;
$progname <user/passw\@instance> <flat file name> (-ddbj | -ncbi | -ebi) [-prefix <prefix> -max <number>]
	
	Parses <flat file name> and writes file(s) containing all entries coming
	  from the specified database.

	-prefix: output file names will be <prefix>.1 <prefix>.2 ....
	         (default to <flat file name>)
					 
	-max:    max number of entries in each output file.
	         (default to $MAX_ENTRIES)

	The format of the flat file (ncbi or ebi) is guessed from that of the first
	  line of the file.
	
	The database connection is used to read the appropriate accno prefixes.

USAGE

	my $conn = shift(@ARGV);
	my $fname = shift(@ARGV);
	
	unless ($fname) {
		die($USAGE);
	}

	my %opt;

	GetOptions ("ddbj"  => \$opt{ddbj},
              "ncbi"  => \$opt{ncbi},
							"ebi"   => \$opt{ebi},
						  "prefix=s"=> \$opt{prefix},
						  "max=i"   => \$opt{max});
	
	my $dbcode;
	if ($opt{ddbj}) {
		
		$dbcode = 'D';
		
	} elsif ($opt{ncbi}) {

		$dbcode = 'G';

	} elsif ($opt{ebi}) {

		$dbcode = 'E';

	} else {

		die ("You must choose one of ddbj, ncbi, ebi\n$USAGE");
	}

	if ($opt{prefix}) {

		$PREFIX = $opt{prefix};
	
	} else {

		$PREFIX = $fname;
	}

	if ($opt{max}) {

		$MAX_ENTRIES = $opt{max};
	
	}
	
	my $prefixes_hr = get_accno_prefixes($conn, $dbcode);
	
	unless (-f $fname) {
		die("File '$fname' does not exists or you don't have permission to read it.\n");
	}

	return($prefixes_hr, $fname);
}


sub get_accno_prefixes {
	#
	#
	#
	my ($dbconn, $dbcode) = @_;

  my $dbh = DBI->connect ('dbi:Oracle:', $dbconn, '',
                          {AutoCommit => 0,
                           PrintError => 1,
                           RaiseError => 1} );
  
	unless ($dbh) {
     die ("Could not connect using '$dbconn'\n$!\n");
  }

	my $sql = <<SQL;
SELECT prefix
  FROM cv_database_prefix
 WHERE dbcode = '$dbcode'	
SQL
	
	my $dbcodes_ar = $dbh->selectcol_arrayref($sql);
	
	unless (@$dbcodes_ar) {
			die("Can't get any dbcode with:\n$sql\n");
	}

	$dbh->disconnect();
	my $dbcodes_hr = {};
	%$dbcodes_hr = map(($_, 1), @$dbcodes_ar);# put everything in a hash with values = 1

	return $dbcodes_hr;
}

