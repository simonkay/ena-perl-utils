#!/ebi/services/tools/bin/perl -w
#
#

my ($pwd, $prog);

BEGIN
{
   my $fname = __FILE__;
   if ($fname =~ m"(^|^.*/)([^/]+)$") {#"
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

my $DEFAULT_FLAGS = '-wgs';

main ();


sub main {

   my ($conn_str, $fnames_ar, $format, $extra_flags) = read_args();

   print(STDERR "LOG: checking prefixes\n");
   my $accno_prefix = check_prefixes($fnames_ar, $format);
   print(STDERR "LOG: prefix is '$accno_prefix'\n");
   
   my ($stored_entries, $failed_entries) = load_entries($conn_str, $fnames_ar, $format, $extra_flags);

   if ($failed_entries > 0) {# some entries failed to load

      print(STDERR "WARNING: $failed_entries entries failed to load.\n" .
                   "WARNING:  the previous set was not killed and the distribution_wgs table\n" .
                   "WARNING:  was not modfied. The entries loaded (if any) will not be distributed.\n" .
                   "WARNING: The full putff output for each file is in '<file name>.putff'\n");

   } elsif ($stored_entries == -10) {

      print(STDERR "WARNING: No entry was stored and no error was reported.\n" .
                   "WARNING: If this is not what you expected (e.g. you did not add the -parse_only\n" .
                   "WARNING:  flag) check the *.putff files.\n");
   
   } else {# the full set was loaded
   
      my $dbh = DBI->connect ('dbi:Oracle:', $conn_str, '',
                             {AutoCommit => 0,
                              PrintError => 1,
                              RaiseError => 1} );
      
      unless ($dbh) {

         die("ERROR: Could not connect using '$conn_str'\n" .
             "ERROR: Oracle response was:\n$!\n" .
             "ERROR: You should fix the problem and manually kill the previous WGS set\n" .
             "ERROR:  and update the distribution_wgs table.\n");
      }

      Wgs::kill_previous_set($dbh, $accno_prefix);
   
      Wgs::add_to_distribution_table($dbh, $accno_prefix);

      $dbh->disconnect();
   }
}


sub check_prefixes {
   #
   #
   #

   my ($fnames_ar, $format) = @_;
   my %all_accno_prefixes;

   foreach my $fname (@$fnames_ar) {
      
      print(STDERR "LOG: $fname\n");
      my ($prefixes_ar, $nofSeqs) = Wgs::get_accno_prefixes_from_file($fname, $format);
      
      if ($#$prefixes_ar) {# more than one prefix in this file
   
         $" = "\nERROR: ";
         die ("ERROR: '$fname' contains entries from different sets:\n" .
              "ERROR: @$prefixes_ar\n" .
              "ERROR: ----------------------------------------------\n");
      
      } else {
   
         foreach (@$prefixes_ar) {

            if ($all_accno_prefixes{$_}) {

               $all_accno_prefixes{$_} .= ", $fname";
            
            } else {

               $all_accno_prefixes{$_} .= $fname;
            }
         }
      }
   }
   
   if ( scalar(keys(%all_accno_prefixes)) > 1 ) {# more than one prefix accross the files

      print(STDERR "ERROR: more than one prefix met.\n");

      while ( my($prefix, $files) = each(%all_accno_prefixes) ) {

         print(STDERR "ERROR: $prefix found in file(s): $files\n");
      }
   }

   my $accno_prefix = (keys(%all_accno_prefixes))[0];

   return $accno_prefix;
}

   
sub load_entries {
   #
   #
   #

   my ($conn_str, $fnames_ar, $format, $extra_flags) = @_;
   my ($stored_total, $failed_total) = (0, 0);

   foreach my $fname (@$fnames_ar) {

      my $err_file = "$fname.putff";
      print(STDERR "LOG: loading '$fname' into $conn_str (putff)\n");
      my $exit_value = Putff::putff("$conn_str $fname $DEFAULT_FLAGS $format $extra_flags", $err_file);
      my ($stored, $failed, $unchanged, $parsed) = Putff::parse_err_file($err_file);
      
      $stored_total += $stored;
      $failed_total += $failed;
      my $summary = Putff::get_summary($err_file);
      print(STDERR "LOG: putff summary:\n" .
                   "$summary\n");
   }
   
   return ($stored_total, $failed_total);
}



sub read_args {
   #
   #
   #


   my ($progname) = $0 =~ m|([^/]+)$|;

   my $USAGE = <<USAGE;
$progname <user/passw\@instance> <file name list> (-embl | -ncbi) <extra putff flags>

   Loads (via putff) a WGS set possibly in more than one file:
     1) Checks that all entries are WGS and have the same accession number prefix.
     2) Calls 'putff <user/passw\@instance> <file name> $DEFAULT_FLAGS (-embl | -ncbi) <extra putff flags>
     3) If all entries were loaded then:
         -) Inserts the accession number prefix into the distribution_wgs table.
         -) Flags as 'suppressed' all entries of the previous set (if there is one).

   IMPORTANT:
     If you are loading a new set you must load it *WHOLE* if you want to distribute it 
      all in one file.
     If you are loading an update to an existing set you should not worry about wholeness.

USAGE

   my $conn_str = shift(@ARGV);
   my $format;
   my @files;
   my $argn = $#ARGV + 1;

   for (my $i = 0; $i < $argn; ++$i) {
      
      my $arg = shift(@ARGV);

      if ($arg =~ m/^-/) {

         $format = $arg;
         last;

      } else {

         push (@files, $arg);
      }
   }

   my $extra_flags = join(' ', @ARGV);


   if ($#files < 0) {

      die("ERROR: you must specify at least one file to load.\n$USAGE\n");
   }

   unless ($format){

      die("ERROR: you must specify the format of the file(s)\n\n$USAGE");
   }

   foreach (@files) {
   
      unless (-r($_) ) {

         die ("ERROR: file '$_' does not exists or it is not readable by you.\n");
      }
   }

   unless ($format eq '-embl' ||
           $format eq '-ncbi' ) {

      die ("ERROR: the format must be one of '-embl' or '-ncbi', you entered '$format'\n\n$USAGE");
   }
print "'$extra_flags'\n";
exit;
   return ($conn_str, \@files, $format, $extra_flags);
}
