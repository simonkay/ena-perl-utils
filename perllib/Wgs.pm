package Wgs;

use strict;
use TempFiles;

=head1 DESCRIPTION

This module provides functions to be used for the handling of WGS files.

=head1 PUBLIC FUNCTIONS

=over 4

=item C<get_accno_prefixes>

   $prefixes_hr = get_wgs_prefixes ($dbh, $dbcode);

Returns a reference to a hash whose keys are all the WGS accession number

I<$dbcode> must be one of: 'E' for EMBL, 'G' for GenBank, 'D' for DDBJ.

=cut

sub get_accno_prefixes {
   #
   #
   #

   my ($dbh, $dbcode) = @_;

   if ($dbcode ne 'E' &&
       $dbcode ne 'G' &&
       $dbcode ne 'D') {

      die ("ERROR: get_wgs_prefixes dbcode must be one of 'E', 'G', 'D'.\n" .
           "ERROR:  supplyed value is '$dbcode'.");
   }

   my $sql = <<SQL;
   SELECT prefix
     FROM cv_database_prefix
    WHERE dbcode = '$dbcode'
      AND length(prefix) = 4
SQL

	my $prefixes_ar = $dbh->selectcol_arrayref($sql);
	my $prefixes_hr = {};
	%$prefixes_hr = map(($_, 1), @$prefixes_ar);# put everything in a hash with values = 1

	return $prefixes_hr;
}


=item C<filter>

   $n_of_seqs = filter($fname);
   $n_of_seqs = filter($fname, $prefixes_hr);

Eliminates 'gene' lines from the feature table of file I<$fname> which
 must be an NCBI flat file. The file is edited 'in place' so make a
 copy of it before calling this function if you want to keep the
 original.

If the hash reference I<$prefixes_hr> is passed this function eliminates
 from the file I<$fname> all entries that have an accession number
 prefix not contained in the B<keys> of I<%$prefixes_hr>.

Dies if a non-WGS entry is met (i.e. an accno not matching
 m/[A-z]{4}\d\d/)

Returns the number of sequences left in the flat file and an arrayref
 containgng all the accno prefixes found.

=cut

sub filter {

  my ($fname, $prefixes_hr) = @_;
  my ($prefix, $prev_prefix, $seqNo, $keep, %prefixlist);
  my $nogood = 0;
  my $AC = 'ACCESSION';

  my $temp_file = temp_file('.', 'wgs_ncbi_temp');

  open (IN, "<$fname") or
    die ("ERROR: Can't open '<$fname'\n$!");

  open (OUT, ">$temp_file") or
    die ("ERROR: Can't open the temporary file '$temp_file'\n$!");

  $seqNo = 0;
  while (<IN>){

    $keep = '';
    while ( $_ && $_ !~ m"^$AC" ){ # get to the AC line

      $keep .= $_; # remember for later
      $_ = <IN>;
    }

    if ($_) {

      if ($_ =~ m"^$AC +([A-Z]{4}\d\d)") {

         $prefix = $1;             # extract the accession number prefix
         $prefixlist{$prefix} = 1;

         if ( !defined $prefixes_hr ||               # no prefix restriction OR
              defined ($prefixes_hr->{$prefix}) ) {  # prefix is OK

           ++$seqNo;
           print (OUT $keep);      # print what we remembered before ...

           while ( $_ && $_ !~ m"^//" ){ # ... and the rest of the sequence file

             print OUT;
             $_ = <IN>;
           }
           print (OUT "//\n");# end of sequence tag

         } else {#  prefix is not in the prefix list
           ++$nogood;

           while ( ($_ = <IN>) !~ m"^//" && $_){ # skip this sequence file
             ;# do nothing
           }
         }

      } else {

         my $line_no = $.;
         close (OUT);
         close (IN);
         unlink ($temp_file);
         chomp;
         die ("ERROR: this file contains non-WGS entries.\n" .
              "ERROR: '$_'\n" .
              "ERROR:  at line $line_no of '$fname'\n");
      }
    }
  }
  close (OUT);
  close (IN);

  my @prefixes_arr = keys(%prefixlist);
  rename ($temp_file, $fname);
  return ($seqNo, \@prefixes_arr);# return number of sequences in file and prefix
}


=item C<get_accno_prefixes_from_file>

    ($prefixes_ar, $n_of_entries) = get_accno_prefixes_from_file($fname, $format);

Parses the file I<$fname> which is in format I<format> and returns an arrayref
to a list of all accession number prefixes (4 letters + 2 digits) in the file
and the total number of entries in the file.

Dies if a non-WGS accession number is met.

I<$format> must be one of 'B<-ncbi>' or 'B<-embl>'

=cut

sub get_accno_prefixes_from_file {

   my ($fname, $format) = @_;

   my $AC;
   if ($format eq '-ncbi') {

      $AC = 'ACCESSION';

   } elsif ($format eq '-embl') {

      $AC = 'AC';
   }

   open (IN, "<$fname") or
      die ("ERROR: Can't open '<$fname'\n$!");

   my ($line, $linetype, $prefix, %prefixlist);
   my $linetype_length = length($AC);
   my $nofSeqs = 0;

   while ($line = <IN>){

      $linetype = substr($line, 0, $linetype_length);

      if ($linetype eq $AC) {

         ++$nofSeqs;

         if ($line =~ m"^$AC +([A-Z]{4}\d\d)") {

            $prefix = $1;             # extract the accession number prefix
            $prefixlist{$prefix} = 1;

         } else {

            my $line_no = $.;
            close (IN);
            chomp;
            die ("ERROR: this file contains non-WGS entries.\n" .
                 "ERROR: '$line'\n" .
                 "ERROR:  at line $line_no of '$fname'\n");
         }

         while (<IN>) {# go to the end of the entry
            chomp;
            last if ($_ eq '//');
         }
      }
   }

   close (IN);

   my @prefixes = keys(%prefixlist);
   return (\@prefixes, $nofSeqs);# return prefixes array and number of sequences in the file
}


=item C<kill_previous_set>

    $entries_killed = kill_previous_set($dbh, $prefix);

Kills the WGS set previous to C<$prefix> (e.g. if I<$prefix> is 'B<CAAC03>'
 all entries with accno like 'B<CAAC02%>' are marked as killed)
Rollbacks and dies if more than 500000 entries are affected.

Returns the number of entries killed.

=cut

sub kill_previous_set {

   my ($dbh, $current_prefix) = @_;

   my ($letters, $numbers);

   if ($current_prefix !~ m/^([A-Z]{4})(\d\d)$/) {

      $dbh->disconnect();
      die ("ERROR: Wgs::kill_previous_set prefix '$current_prefix' does not match".
           "ERROR:  WGS accno prefix format ([A-Z]{4}\\d\\d).\n");

   } else {

      ($letters, $numbers) = ($1, $2);

      if ($numbers <= 1) {
         # this is the first set, there is nothing to suppress
         print(STDERR "LOG: there is no previous set to suppress.\n");
         return;
      }

      --$numbers;

      my $old_prefix = sprintf("%s%02d", $letters, $numbers);

      if ($old_prefix !~ m/^[A-Z]{4}\d\d$/) {

         die ("ERROR: Wgs::kill_previous_set\n" .
              "ERROR: the prefix of the set to kill results as '$old_prefix'\n ");
      }

      my $set_remark = "begin auditpackage.remark := 'no distribution - WGS set killed'; end;";

      $dbh->do($set_remark);

      my $sql = <<SQL;
UPDATE dbentry
   SET confidential = 'N',
       entry_status = 'D'
 WHERE primaryacc# like '$old_prefix%'
SQL

      my $rownum = $dbh->do($sql);

      if ($rownum > 1000000) {# too many rows affected, it does not look possible

         $dbh->rollback();
         print(STDERR "WARNING: Wgs::kill_previous_set\n" .
                      "WARNING: killing the previous set affects $rownum rows.\n" .
                      "WARNING: This number seems too high so a rollback was issued.\n" .
                      "WARNING: The SQL in question is:\n$sql\n".
                      "WARNING: ----------------------------------------------------\n");

      } else {# a reasonable number of row affected -> commit

         print(STDERR "LOG: $rownum entries with prefix '$old_prefix' suppressed.\n");
         $dbh->commit();
      }

      return $rownum;
   }
}


=item C<add_to_distribution_table>

    add_to_distribution_table($dbh, $prefix);

Adds the prefix I<$prefix> to the I<datalib.distribution_wgs> table
so that the WGS set with accnos beginning with I<$prefix> will be distributed
next time round.

I<$prefix> must match m/[A=Z]{4}\d\d/ or the function will die()

=cut

sub add_to_distribution_table {

   my ($dbh, $prefix) = @_;

   if ($prefix !~ m/[A-Z]{4}\d\d/) {

      $dbh->disconnect();
      die ("ERROR: Wgs::add_to_distribution_table\n" .
           "ERROR: prefix must match m/[A-Z]{4}\\d\\d/\n" .
           "ERROR: the prefix passed is '$prefix'.\n");
   }

   my $sql = <<SQL;
INSERT INTO datalib.distribution_wgs
       (wgs_set,   distributed)
VALUES ('$prefix', 'N')
SQL

   my $rownum = $dbh->do($sql);

   if ($rownum !=1) {

      $dbh->rollback();
      $dbh->disconnect();
      die ("ERROR: Wgs::add_to_distribution_table\n" .
           "ERROR: inserting one value affects $rownum rows\n" .
           "ERROR: the SQL statement is :\n$sql\n" .
           "ERROR: ---------------------------------------\n");
				
   } else {

      print(STDERR "LOG: adding '$prefix' to the distribution table.\n");
			$dbh->commit();
		}
}

1;

=back


