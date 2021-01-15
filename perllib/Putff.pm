package Putff;

use strict;
use Utils qw(my_open);

=head1 DESCRIPTION

This module provides functions to be used for sending files to putff and for
 parsing putff output.

=head1 PUBLIC FUNCTIONS

=over 4

=item C<putff>

   $exit_value = putff($args, $err_file);
   $exit_value = putff($args);

Calls putff as 'putff I<$args> > I<$err_file> 2>&1' or 'putff I<$args>'
 depending on whether I<$err_file> is passed or not.

I<$args> is not checked for shell meta characters so nasty things can happen if 
 you do something like: 'putff("\nrm *");'
ToDo: treat I<$args> as tainted.

=cut

sub putff {

   my ($args, $err_file) = @_;

   my $cmd = "/ebi/services/tools/seqdb/bin/putff $args";

   if ($err_file) {

      $cmd .= " > $err_file 2>&1";
   }

   return system ($cmd);
}

=item C<parse_err_file>

   putff("/@prdb1", $err_file);
   ($stored, $failed, $unchanged, $parsed) = parse_err_file($err_file);

Returns the number of stored, failed, unchanged, parsed entries as reported in
 the putff error file I<$err_file> which is assumed to finish with:

entries:
  stored    =    x
  failed    =    x
  unchanged =    x
  ----------------
  parsed    =    x

=cut

sub parse_err_file {

   my ($err_file) = @_;

   my $summary = get_summary($err_file);
   
   my @summary_lines = split(/\n/, $summary);

   my ($stored, $failed, $unchanged, $parsed);
   foreach (@summary_lines) {

      if (m/stored += +(\d+)/) {
         
         $stored = $1;
      
      } elsif (m/failed += +(\d+)/) {
         
         $failed = $1;

      } elsif (m/unchanged += +(\d+)/) {

         $unchanged = $1;

      } elsif (m/parsed += +(\d+)/) {

         $parsed = $1
      }
   }

   unless( defined($stored) &&
           defined($failed) &&
           defined($unchanged) &&
           defined($parsed)       ) {
      
      die ("ERROR: Putff::parse_err_file\n" .
           "$summary" .
           "does not look like a putff output.\n");
   }

   return ($stored, $failed, $unchanged, $parsed);
}

=item C<get_summary>

   putff('/@prdb1 myfile', $err_file);
   $loading_summary = get_summary($err_file);

Returns the last 8 lines of the file named I<$err_file>.
It is just a wrapper to `tail -n8 $err_file`.

=cut

sub get_summary {

   my ($err_file) = @_;

   my $summary = `tail -n8 $err_file`;

   return $summary;
}

=item C<print_report>

   putff("/\@prdb1 $myfile -summary");
   open( OUT, '>loading_report' );
   
   print_report( \*OUT, $myfile, $dbh);
   
   close( OUT );

Prints a report parsing the xml summary produced by putff.
If a database handle is supplyed as the third argument
the sequence version of the entry is included in the report.
Expects to find the file "I<$myfile.summary.xml>".

Prints Entry name, accession number, ac star, (LOADED|FAILED)

If the $dbh parameter is supplyed (and it is an open database
hanlde to prdb1 or devt) the report includes also the state and
sequence versiuon number of the loaded entry.

=cut

sub print_report {

  my ( $out_fh, $fname, $dbh ) = @_;
  
  use XparserDOM;

  my $nof_entries = 0;
  
  if ( $dbh ) {

    my $stmt = __get_entry_details_stmt( $dbh );
    Xparse("$fname.summary.xml", {'entry' => sub {__print_entry_report_full(\$nof_entries, $out_fh, $dbh, $stmt, @_)}});

  } else {

    Xparse("$fname.summary.xml", {'entry' => sub {__print_entry_report_short(\$nof_entries, $out_fh, @_)}});
  }

  return $nof_entries;
}


sub get_report_header_long {

  "#                          Automated Sequence loading report\n".
  "#ID            AC               AC *                                           State      Version\n".
  "#------------  ---------------  ---------------------------------------------  ---------- -------\n";
}


=item C<scan_summary>

   putff("/\@prdb1 $my_file -summary");

   $nof_entries = scan_summary( $my_file, \&my_sub );

Scans the XML summary from putff, for each entry it will call C<my-sub( $entry_data )>
where $entry_data is a reference to a hash like this:
  {ID       => 'AB123456',  # the entry name
   AC       => 'ZY999999',  # the accession number
   AC_STAR  => '_LKJH3879', # the genome project id
   WARNINGS => "WARNING: this is dodgy\n". # The warnings
               "WARNING: blady blah",      # each line begins with 'WARNING: '
   ERRORS   => "ERROR: this is wrong because\n". # The errors
               "ERROR: something or other",      # each line begins with 'ERROR: '
   STATUS   => ('failed'|'stored'|'unchanged'|'not stored: user requested rollback'),
   LOADING  => ('LOADED'|'NOT LOADED') }

The WARNINGS and ERRORS values are wrapped at coulmn 97, each line will begin
with 'WARNING: ' or 'ERROR: '.

It assumes the presence of the file "$my_file.summary.xml", it dies if it does
not exists.

RETURNS: the number of entries in the report.

=cut

sub scan_summary {
  #
  #
  #
  my ( $fname, $callback ) = @_;

  use XparserDOM;

  my $n_of_entries = 0;
  
  Xparse( "$fname.summary.xml", { entry => sub{ ++$n_of_entries;
                                                my $entry_data = __get_entry_data( @_ );
                                                &$callback( $entry_data );              } } );
  return $n_of_entries;
}


sub __get_entry_data {

  my ( $entry_ref ) = @_;


  my ($id, $ac, $ac_star);
  my ($errors, $warnings, $status) = ('', '', '');

  use Text::Wrap qw(wrap);
  $Text::Wrap::columns = 97;

  my $STATUS_FAILED = 'failed';

  $id = $$entry_ref{children}{id}[0]{content};
  if (defined ($id)) {
    $id  =~ s/^\s*(\S.*\S)\s*$/$1/;

  }else{
    $id = '-';
  }

  $ac = $entry_ref->{children}{accession_number}[0]{content};
  if (defined ($ac)) {
    $ac  =~ s/^\s*(\S.*\S)\s*$/$1/;

  }else{
    $ac = '-';
  }

  $ac_star = $entry_ref->{children}{genome_project}[0]{content};
  if (defined ($ac_star)) {
    $ac_star  =~ s/^\s*(\S.*\S)\s*$/$1/;

  }else{
    $ac_star = '-';
  }

  $status = $$entry_ref{children}{status}[0]{content};
  $status =~ s/^\s*(\S.*\S)\s*$/$1/;

  foreach ( @{$entry_ref->{children}{warnings}[0]{children}{warning}} ){

    my $line = $_->{content};

    $line =~ s/^\s*(\S.*\S)\s*$/$1/; # trim whitespace
    $line =~ s/\s\s+/ /g; # compress whitespaces

    if ($line) {

      $warnings .= join( '', wrap( "WARNING: ", "WARNING: ", "$line\n" ) );
    }
  }

  my $loading;
  if ($status eq $STATUS_FAILED) {# failed submission

    foreach ( @{$entry_ref->{children}{errors}[0]{children}{error}} ){

      my $line = $_->{content};

      $line =~ s/^\s*(\S.*\S)\s*$/$1/;
      $line =~ s/\s\s+/ /g; # compress whitespaces

      if ($line) {

        $errors .= join( '', wrap( "ERROR: ", "ERROR: ", "$line\n" ) );
      }
    }
    $loading = 'FAILED';
  
  } else {

    $loading = 'LOADED';
  }

  return  {ID       => $id,
           AC       => $ac,
           AC_STAR  => $ac_star,
           WARNINGS => $warnings,
           ERRORS   => $errors,
           STATUS   => $status,
           LOADING  => $loading};
}
       

sub __print_entry_report_short {
  
  my ( $nof_entries_sr, $out_fh, $entry_ref ) = @_;
  
  ++$$nof_entries_sr;
  
  my $entry_data =  __get_entry_data( $entry_ref );

  format REPORT =
@<<<<<<<<<<<<  @<<<<<<<<<<<<<<  @<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<  @<<<<<<<<
$entry_data->{ID},  $entry_data->{AC},    $entry_data->{AC_STAR},              $entry_data->{LOADED}                     
.

  select((select($out_fh), $~ = 'REPORT')[0]);

  write($out_fh);

  if ( $entry_data->{WARNINGS} ) {
    
    print( $out_fh $entry_data->{WARNINGS} );
  }

  if ( $entry_data->{ERRORS} ) {
    
    print( $out_fh $entry_data->{ERRORS} );
  }

  print( $out_fh "\n" );
}


sub __print_entry_report_full {
  
  my ($nof_entries_sr, $out_fh, $dbh, $stmt, $entry_ref) = @_;

  ++$$nof_entries_sr;

  my $entry_data =  __get_entry_data( $entry_ref );

  $stmt->execute( $entry_data->{AC}, $entry_data->{AC} );
  
  ( $entry_data->{ID}, $entry_data->{VERSION}, $entry_data->{STATE}, $entry_data->{LENGTH} ) = $stmt->fetchrow_array();

  format REPORT_LONG =
@<<<<<<<<<<<<  @<<<<<<<<<<<<<<  @<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<  @<<<<<<<<< @<<<<<<
$entry_data->{ID}, $entry_data->{AC}, $entry_data->{AC_STAR}, $entry_data->{STATE}, $entry_data->{VERSION}
.

  select((select($out_fh), $~ = 'REPORT_LONG')[0]);

  write($out_fh);

  if ( $entry_data->{WARNINGS} ) {
    
    print( $out_fh $entry_data->{WARNINGS} );
  }

  if ( $entry_data->{ERRORS} ) {
    
    print( $out_fh $entry_data->{ERRORS} );
  }

  print( $out_fh "\n" );
}


sub __get_entry_details_stmt {

  my ($dbh) = @_;
  
  my $sql = <<SQL_END;
SELECT
       de.entry_name,
       bs.version,
       'Unfinished' "State",
       bs.seqlen
  FROM
        datalib.dbentry de,
        datalib.bioseq bs
  WHERE
        de.primaryacc# = ?
    AND bs.seqid = de.bioseqid
    AND EXISTS (
                SELECT
                       1
                  FROM
                       datalib.dbentry_keyword dbk,
                       datalib.keyword kw
                  WHERE
                       dbk.keywordid = kw.keywordid
                   AND dbk.dbentryid = de.dbentryid
                   AND kw.keyword LIKE 'HTGS_PHASE%'
                )
UNION
SELECT
       de.entry_name,
       bs.version,
       'Finished' "State",
       bs.seqlen
  FROM
       datalib.dbentry de,
       datalib.bioseq bs
  WHERE
       de.primaryacc# = ?
   AND bs.seqid = de.bioseqid
   AND NOT EXISTS (
                   SELECT
                          1
                     FROM
                          datalib.dbentry_keyword dbk,
                          datalib.keyword kw
                     WHERE
                          dbk.keywordid = kw.keywordid
                      AND dbk.dbentryid = de.dbentryid
                      AND kw.keyword LIKE 'HTGS_PHASE%'
                   )
SQL_END


  my $stmt = $dbh->prepare ($sql);

  return $stmt;
}



=item C<get_accnos>

   putff("/\@prdb1 $myfile -summary");
   my ( $loaded, $not_loaded ) = get_accnos( $myfile );

Returns an array ref to all the accnos loaded and another arrayref to all
accnos that have not been loaded for whatever reason.
Expects to find the file "I<$myfile.summary.xml>".

=cut

sub get_accnos {

  my ($fname) = @_;

  use XparserDOM;

  my ($loaded_ar, $not_loaded_ar) = ([], []);

  Xparse("$fname.summary.xml", {'entry' => sub {__push_accnos($loaded_ar, $not_loaded_ar, @_)}});
  
  return ( $loaded_ar, $not_loaded_ar );
}



sub __push_accnos {
  #
  #
  #

  my ($loaded_ar, $not_loaded_ar, $entry_ref) = @_;

  my $STATUS_FAILED = 'failed';

  my $ac = $entry_ref->{children}{accession_number}[0]{content};
  if (defined ($ac)) {
    $ac  =~ s/^\s*(\S.*\S)\s*$/$1/;

  }else{
    $ac = '-';
  }

  my $status = $entry_ref->{children}{status}[0]{content};
  $status =~ s/^\s*(\S.*\S)\s*$/$1/;

  if ($status eq $STATUS_FAILED) {# failed submission

    push( @$not_loaded_ar, $ac );

  } else {# succesful submission

    push( @$loaded_ar, $ac );

  }
}

1;

=back


