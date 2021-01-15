#!/ebi/services/tools/bin/perl -w

=head1
check_ncbi_wgs.pl - Parses and checks an NCBI WGS file

=head1 DESCRIPTION

This program is little more than a wrapper for:
C<fixncbi.pl>
C<duplication.pl>
C<putff -parse_only>

=cut


my ($pwd, $prog);

# the following bit is so that if you are in your development environment
# you use your libraries and if this is in the production directory it uses
# the production libraries
BEGIN
{
   my $fname = __FILE__;
   if ($fname =~ m'(^|^.*/)([^/]+)$') {
      $pwd = $1;
      $prog = $2;

   }else{
      die ("Cannot figure out where I am!\n ");
   }
}

use strict;
use lib "${pwd}../perllib";
use Wgs;
use Putff;
use DBI;

my $DEFAULT_FLAGS = '-wgs -ncbi -parse_only -no_error_file';

main ();


sub main {
   #
   #
   #

   my ($conn_str, $fname, $extra_flags) = read_command_line();

   my $fixer = '/ebi/services/tools/bulkload/scripts/fixncbi.pl';
   my $duplicates = '/ebi/services/tools/bulkload/scripts/duplication.pl';

   
   print (STDERR "LOG: Connecting to $conn_str.\n");
   my $dbh = DBI->connect ('dbi:Oracle:', $conn_str, '',
                           {AutoCommit => 0,
                            PrintError => 1,
                            RaiseError => 1} );

	unless ($dbh) {
     die ("ERROR: Could not connect using '$conn_str'\n$!\n");
   }
   
   my $accno_prefixes_hr = Wgs::get_accno_prefixes($dbh, 'G');
   $dbh->disconnect();

	 my ($accno_prefixes_ar, $n_of_entries) = Wgs::get_accno_prefixes_from_file($fname, '-ncbi');
	 
	 if ($#$accno_prefixes_ar) {

      $dbh->disconnect();
      $" = "\nERROR: ";
      die ("ERROR: '$fname' contains entries from different sets:\n" .
           "ERROR: @$accno_prefixes_ar\n" .
           "ERROR: ----------------------------------------------\n");
   }

   if (0 == $n_of_entries) {

      die ("ERROR:  '$fname' does not contain any NCBI WGS entry.\n");
   }

   print (STDERR "LOG: Fixing known problems.\n");
   my_system ("$fixer $fname", __LINE__);

   print (STDERR "LOG: Merging duplicate features.\n");
   my_system ("$duplicates $fname", __LINE__);
   rename ("${fname}_out", $fname)
     or die("Can't rename '${fname}_out' to '$fname'\n");

   print (STDERR "LOG: Error checking (full putff output in '$fname.putff').\n");
   Putff::putff ("$conn_str $fname $DEFAULT_FLAGS $extra_flags", "$fname.putff");
   print (STDERR Putff::get_summary ("$fname.putff"));
   print (STDERR "LOG: Done!\n\n");
}



sub my_system {
   # executes a system commend and dies on error
   #
   #

   my ($cmd, $line_no) = @_;

   system ($cmd);
   if ($?) {
      
      print (STDERR "ERROR: can't execute: '$cmd'");

      if ($line_no) {

         print (STDERR  "at: $line_no\n");
      }
      exit $?;
   }
}


sub read_command_line {
   #
   #
   #
   
   my ($progname) = $0 =~ m|([^/]+)$|;

   my $USAGE = <<USAGE;
$progname <user/passw\@instance> <file name> <extra flags>

  1) removes all non-NCBI and non-WGS entries from the file
  2) calls fixncbi.pl
  3) calls duplication.pl
  4) calls 'putff <user/passw\@instance> <file name> $DEFAULT_FLAGS <extra flags>'
       
  NOTE:
    The file is modified in place, make a copy of it beforehand if you want to
     keep the original.
     
    The file must contain entries with the same accession number prefix, if two
     different prefixes are met during parsing an ERROR is reported and the 
     program stops.

USAGE

   my $conn = shift(@ARGV);
   my $fname = shift(@ARGV);
   my $extra_flags = join(' ', @ARGV);

   unless ($fname) {
      die($USAGE);
   }
   
   unless (-f ($fname) &&
           -r ($fname) ) {

      die ("ERROR: file '$fname' does not exists or it is not readable by you.\n");
   }

   return ($conn, $fname, $extra_flags);
}

