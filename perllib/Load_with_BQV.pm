package Load_with_BQV;


use strict;
use Utils qw(my_open my_die);
use Putff qw(putff scan_summary);
use DBI;
use DBD::Oracle qw(:ora_types);# needed to load the BQVs blob

my $_BQV_SUFFIX = '.bqv';
my $_SEQ_SUFFIX = '.seq';
my $_REPORT_SUFFIX = '.report';


sub new {
  my ( $self, $fname ) = @_;

  my %the_obj;
  %the_obj = ( fname         => $fname,
               bqv_fname     => $fname. $_BQV_SUFFIX,
               seq_fname     => $fname. $_SEQ_SUFFIX,
               report_fname  => $fname. $_REPORT_SUFFIX,
               summary_fname => $fname. $_SEQ_SUFFIX. '.summary.xml'  );

  return bless( \%the_obj, $self );
}


=head1 DESCRIPTION

This module provides a function used for loading flat files containing BQVs.
They mostly come from project submissions.

=head1 PUBLIC FUNCTIONS

=over 4

=item C<load_with_bqvs>

  my $loader = Load_with_BQV->new( $fname );
  $loader->load_with_bqv( $dbh, $login, $extra_args, $opts_hr );

Loads the flatfile C<$fname> into the database to which C<$dbh> and C<$login> point.
C<$extra_args> are extra arguments to be passed to putff for loading ( B<-summary> is passed by default ).
The hashref C<$opts_hr> contains options that can switch off some steps in the procedure:

 {
  -no_load_seq => 1, # The file is not loaded, it is assumed that it
                     #  has been already loaded with the -summary switch and
                     #  the files "$fname.dat.summary.xml" and "$fname.bqv"
                     #  exist.
  -no_load_bqv => 1, # The file is parsed and loaded, BQVs are extracted but
                     #  not loaded.
  -no_parse    => 1, # The file is not parsed, BQVs are not extracted. It is
                     #  assumed that the files "$fname.dat" and "$fname.bqv"
                     #  already exist.
  -no_report   => 1  # The report is not written.
 }

Several files are written:

  "$fname.seq"   -> Is "$fname" with the BQVs taken out.
  "$fname.bqv"   -> Contains all BQVs taken out of "$fname" in a fasta-like format.
                     Entries without BQVs will have a corresponding empty entry here.
  "$fname.seq.summary.xml" -> Is the loading summary written by putff in XML format
  "$fname.report" -> Contains the loading report in GPscan format.

=cut

sub load_with_bqv {

  my ( $self, $dbh, $login, $extra_args, $opts_hr ) = @_;

  my %opt_proto = ( -no_load_seq => 1,
                    -no_load_bqv => 1,
                    -no_parse    => 1,
                    -no_report   => 1 );

  foreach my $opt ( keys( %$opts_hr ) ) {

    unless( defined( $opt_proto{$opt} ) ) {
      my_die( "Unknown options '$opt'\n" );
    }
  }
  
  if ( !$opts_hr->{-no_parse} ) {
    $self->extract_bqvs();
    # Puts the BQVs in $bqv_fname and the entries in $parsed_fname
  }

  if ( !$opts_hr->{-no_load_seq} ) {
    my $fname = $self->{seq_fname};
    my $putff_error = Putff::putff( "$login $fname $extra_args -summary", "$fname.putff" );
    
    if ( $putff_error ) {
      die( "Error loading '$fname', the output from putff is in '$fname.putff'\n" );
    }
  }

  
  my ( $bqv_action, $report_action );

  if ( !$opts_hr->{-no_load_bqv} ) {# Do load BQVs

    if ( !-e( $self->{bqv_fname} ) ) {
      die( "'$self->{bqv_fname}' does not exist. It is needed for BQV loading.\n" );
    }
    if ( !-e( $self->{summary_fname} ) ) {
      die( "'$self->{summary_fname}' does not exist. It is needed for BQV loading.\n" );
    }

    $bqv_action = \&load_bqv_for_entry;

  } else {

    $bqv_action = sub {};
  }

  if ( !$opts_hr->{-no_report} ) {# Do print report

    if ( !-e( $self->{summary_fname} ) ) {
      die( "'$self->{summary_fname}' does not exist. It is needed for report writing.\n" );
    }

    $report_action = \&write_to_report;

  } else {

    $report_action = sub {};
  }

  if ( !$opts_hr->{-no_report} or !$opts_hr->{-no_load_bqv} ) {
    $self->load_all_bqvs( $dbh, $bqv_action, $report_action );
    # Load BQVs and write report if requested
  }
}


=item C<get_report_file_name>

  my $fname = $loader->get_report_fname();
  
Returns the name of the report file linked to the loader.

=cut

sub get_report_file_name {
  my ($self) = @_;

  return $self->{report_fname};
}


=item C<get_summary_file_name>

  my $fname = $loader->get_summary_fname();
  
Returns the name of the xml summary file linked to the loader.

=cut

sub get_summary_file_name {
  my ($self) = @_;

  return $self->{summary_fname};
} 


=item C<get_data_file_name>

  my $loader = Load_with_BQV->new( $fname );
  my $fname2 = $loader->get_data_fname();
  # $fname 2 is the same as $fname

Returns the name of the data file linked to the loader.
i.e. the same as used in the constructor.

=cut

sub get_data_file_name {
  my ($self) = @_;

  return $self->{fname};
}

=item C<get_putff_file_name>

  my $loader = Load_with_BQV->new( $fname );
  my $fname2 = $loader->get_putff_fname();

Returns the name of the putff output file linked to the loader.

=cut

sub get_putff_file_name {
  my ($self) = @_;

  return $self->{seq_fname} .'.putff';
} 


sub extract_bqvs {
  # Extracts BQVs from $fname
  # Each sequence entry in $fname will have a corresponding BQV entry in $fname.bqv
  # this may be empty if the original sequence entry did not have BQVs.
  # A BQV entry is in fastA-like format, it start with a header line '>' and ends with a
  # '//' line. The header line may contain the method used to compute the quality vales
  # (Phrap or Phred).
  # 
  # The sequence entries are copyed to $fname.dat without their BQVs.
  # 
  my ( $self ) = @_;

  my $in_fh = my_open( $self->{fname} );
  my $seq_fh = my_open( ">$self->{seq_fname}" );
  my $bqv_fh = my_open( ">$self->{bqv_fname}" );
  my $out_fh = $seq_fh;
  my $ac_star;

  my $has_bh  = 0;
  my $has_bqv = 0;

  while( <$in_fh> ) {

    my $line_type = substr ($_, 0, 2);

    if ($line_type eq 'ID'){ # ID

      print( $bqv_fh '>' );# Hold a place for BQVs
      $has_bh  = 0;
      $has_bqv = 0;

    } elsif ( $line_type eq 'BH' ) { # Base Quality Values Header
      # Tells us whether it is Phrap or Phred

      $_ = substr( $_, 5 );
      $has_bh  = 1;
      $out_fh = $bqv_fh;

    } elsif ( $line_type eq 'BQ' ) { # Base Quality Values

      $_ = substr( $_, 5 );
      if ( !($has_bh or $has_bqv) ) {
        print( $bqv_fh "\n" );
      }
      $has_bqv = 1;
      $out_fh = $bqv_fh;

    } elsif ($line_type eq '//') {# end of entry

      $out_fh = $seq_fh;
      if ( !($has_bh or $has_bqv) ) {
        print( $bqv_fh "\n");
      }
      print( $bqv_fh "//\n");

      $has_bh  = 0;
      $has_bqv = 0;
    }

    print( $out_fh $_ );
  }

  close( $in_fh );
  close( $bqv_fh );
  close( $seq_fh );
}


sub load_all_bqvs {
  # Loads all BQVs in $bqv_fname belonging to entries in $seq_fname.
  # A 1:1 correspondence between sequence entries and BQV entries is assumed.
  # That is: for each entry in the .dat file there must be an entry in the .bqv file
  #  if the sequence did not have BQVs an empty entry is expected.
  # BQVs are matched to the entries by position. e.g. the third BQV entry is assumed to
  #  refer to the third sequece entry.

  my ( $self, $dbh, $bqv_action_sr, $report_action_sr ) = @_;

  my $stmts = prepare_statements( $dbh );

  my $bqv_fh = my_open( $self->{bqv_fname} );
  my $report_fh = my_open( ">$self->{report_fname}" );
  print( $report_fh Putff::get_report_header_long() );

  eval {
    Putff::scan_summary( $self->{seq_fname}, sub {
                                               &$bqv_action_sr( $stmts, $bqv_fh, @_ );
                                               &$report_action_sr( $stmts, $report_fh, @_ );
                                             }
                        );
  };

  close( $bqv_fh );
  close( $report_fh );

  foreach my $stmt ( values( %$stmts ) ){
    $stmt->finish();
  }

  if ( $@ ) { # Fatal exception

    $dbh->rollback(); # rollback _ALL_ BQV loading

    die( "NO BQV loaded.\n$@" );

  } else {

    $dbh->commit();
  }
}


sub load_bqv_for_entry {
  # Load BQVs for the entry in $entry_data and writes a report if needed.

  my ( $stmts, $bqv_fh, $entry_data ) = @_;

  my $bqv_data;
  
  $@ = '';
  eval {
    $bqv_data = extract_bqv_file( $bqv_fh );
  };

  if ( $@ eq '' ) {

    if ( $entry_data->{LOADING} eq 'LOADED' ) {

      add_sequence_details( $entry_data, $stmts );
      load_bqv_file( $stmts, $entry_data, $bqv_data );
      unlink( $bqv_data->{FNAME} );
    }
    
  } else {
    $entry_data->{ERRORS} .= format_error( $@ );
  }
}


sub add_sequence_details_with_state {
  # Adds Sequence length, sequence version, state (Finished|Unfinished) to the
  # $entry_data hash
  # The state info is meaningful only for genome project entries.
  # 
  my ( $entry_data, $stmt ) = @_;

  $stmt->{get_seq_details_with_state}->execute( $entry_data->{AC}, $entry_data->{AC} );
  ( $entry_data->{SEQLEN}, $entry_data->{SEQ_VERSION}, $entry_data->{STATE} ) = $stmt->{get_seq_details_with_state}->fetchrow_array();
}


sub add_sequence_details {
  # Adds Sequence length, sequence version to the
  # $entry_data hash
  # 

  my ( $entry_data, $stmt ) = @_;

  $stmt->{get_seq_details}->execute( $entry_data->{AC} );
  ( $entry_data->{SEQLEN}, $entry_data->{SEQ_VERSION} ) = $stmt->{get_seq_details}->fetchrow_array();
}


sub write_to_report {
  # Writes entry info to the loading report
  # Used for genome project entries
  # 
  my ( $stmts, $report_fh, $entry_data ) = @_;

  if ( $entry_data->{LOADING} eq 'LOADED' ) {

    add_sequence_details_with_state( $entry_data, $stmts );

  } else {

    $entry_data->{STATE} = 'Not Loaded';
    $entry_data->{SEQ_VERSION} = '-';
  }

  printf($report_fh "%-14s %-16s %-46s %-10s %s\n", $entry_data->{ID},
                                                    $entry_data->{AC},
                                                    $entry_data->{AC_STAR},
                                                    $entry_data->{STATE},
                                                    $entry_data->{SEQ_VERSION} );

  if ( $entry_data->{WARNINGS} ) {
    print( $report_fh $entry_data->{WARNINGS} );
  }

  if ( $entry_data->{ERRORS} ) {
    print( $report_fh $entry_data->{ERRORS} );
  }

  print ($report_fh "\n");
}


sub prepare_statements {
  # Prepares a few SQL statements used for BQV loading and reporting.
  # 
  my( $dbh ) = @_;

  my %statements;

  my $get_seq_details_sql = "SELECT bs.seqlen, bs.version
                               FROM bioseq bs,
                                    dbentry dbe
                              WHERE bs.seqid = dbe.bioseqid
                                AND dbe.primaryacc# = ?";
  $statements{get_seq_details} = $dbh->prepare( $get_seq_details_sql );


  my $get_seq_details_with_state_sql = "SELECT
                                               bs.seqlen,
                                               bs.version,
                                               'Unfinished' \"State\"
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
                                               bs.seqlen,
                                               bs.version,
                                               'Finished' \"State\"
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
                                                           )";
  $statements{get_seq_details_with_state} = $dbh->prepare( $get_seq_details_with_state_sql );


  my $get_bqv_details_sql = "SELECT bq.seq_accid, bq.chksum, bq.bqv_length
                               FROM datalib.base_quality bq
                              WHERE bq.seq_accid = ?";
  $statements{get_bqv_details} = $dbh->prepare( $get_bqv_details_sql );


  my $update_bqv_details_sql = "UPDATE datalib.base_quality
                                   SET version = ?,
                                       base_quality = ?,
                                       chksum = ?,
                                       bqv_length = ?,
                                       descr = ?
                                 WHERE seq_accid = ?";
  $statements{update_bqv_details} = $dbh->prepare( $update_bqv_details_sql );

  my $insert_bqv_details_sql = "INSERT into datalib.base_quality
                                      (seq_accid, version, base_quality, chksum, bqv_length, descr)
                                VALUES
                                      (?, ?, ?, ?, ?, ?)";
  $statements{insert_bqv_details} = $dbh->prepare( $insert_bqv_details_sql );

  return \%statements;
}


sub extract_bqv_file {
  # Extracts the next BQV entry from $in_fh and computes some statistics:
  # Length, Max value, Min value.
  # The data ready for loading are saved in a temp file.
  # Dies if an invalid format is detected
  # 
  my ( $in_fh ) = @_;

  my $bqv_nof_col = 25;

  my $method = '';
  my $bqv_len = 0;
  my $error = 0;
  my $max = 0;
  my $min = 100;
  my $temp_bqv_file = 'tmp_basequal';

  my $out_fh = my_open( ">$temp_bqv_file" );

  my $line;

  while ( defined($line = <$in_fh>) && $line !~ m/^>/ ) {};

  $method = substr( $line, 1, 5 );
  $method = ucfirst( lc($method) );

  if ( $method ne "\n" ) {

    unless( $method eq 'Phrap' ||
            $method eq 'Phred'    ) {

      close ($out_fh );
      die( "Invalid method in BH line: '$method'\nBQVs not loaded.\n" );
    }

  } else {
    # At a certain point we will not allow to submit without explicitly stating a method

    $method = 'Phrap';
    # Deafult.
  }

  $line = <$in_fh>;

  while ( $line && $line ne "//\n" ){

    # change na ( NA, Na.. ) to zero
    $line =~ s/na/0/ig;

    # delete trailing + preceding blanks
    $line =~ s/^\s+//;
    $line =~ s/\s+$//;

    # now there must not be any non digits left
    if ( $line =~ /[^\d\s]/ ) {

      close ($out_fh );
      die( "Non numeric values found\n$line\nBQVs not loaded.\n" );

    } else {
      # reformat, calc length and collect min and mix value for description line
      my ( @arr ) = split ( /\s+/, $line );

      foreach my $val ( @arr ) {
        printf( $out_fh "%3d", $val );
        if ( ! ( ++$bqv_len % $bqv_nof_col )) {
          print( $out_fh "\n" );
        }

        # collect min and mix value
        if ( $val != 0 ){ # do not record 0 as a minimum
          if ( $val < $min ) {
            $min = $val;
          }
          if ( $val > $max ) {
            $max = $val;
          }
        }
      }
    }

    $line = <$in_fh>;
  }

  print( $out_fh "\n" ) if ( $bqv_len % $bqv_nof_col != 0 );
  close( $out_fh );

  return( {METHOD    => $method,
           LENGTH    => $bqv_len,
           MAX_VALUE => $max,
           MIN_VALUE => $min,
           FNAME     => $temp_bqv_file } );
}


sub load_bqv_file {
  # Loads BQVs from $bqv_data into the database.
  # 
  # 
  my ( $stmt, $entry_data, $bqv_data ) = @_;

  if ( $bqv_data->{LENGTH} == 0 ) {

    return;
  }

  my $fname = $bqv_data->{FNAME};

  # --- check if calculated length is same as seqlen -------------------------
  if ( $entry_data->{SEQLEN} != $bqv_data->{LENGTH} ) {

    my $error = format_error( "'$entry_data->{AC}': sequence length and number of BQVs not identical.\nNot loaded." );
    print( STDERR $error );
    $entry_data->{ERRORS} .= $error;
    return;

  } else {

    #--- calculate checksum ----------------------------------------------------
    my ($cksum) = `crc64 -f $fname`;
    chomp( $cksum );
    my $bqv_blob = `gzip -c $fname`;

    # --- create description line ----------------------------------------------
    my $descr = $bqv_data->{METHOD} ." Quality (Length:$bqv_data->{LENGTH},".
                " Min: $bqv_data->{MIN_VALUE}, Max: $bqv_data->{MAX_VALUE})";

    #--- retrieve bqv info for acc from database -------------------------------
    $stmt->{get_bqv_details}->execute ( $entry_data->{AC} );
    my ( $db_cksum, $db_len ) = $stmt->{get_bqv_details}->fetchrow_array();

    if ( !defined( $db_cksum ) ) {# entry is not in he DB -> insert

      eval {
         $stmt->{insert_bqv_details}->bind_param( 1, $entry_data->{AC} );
         $stmt->{insert_bqv_details}->bind_param( 2, $entry_data->{SEQ_VERSION} );
         $stmt->{insert_bqv_details}->bind_param( 3, $bqv_blob, {ora_type => ORA_BLOB} );
         $stmt->{insert_bqv_details}->bind_param( 4, $cksum );
         $stmt->{insert_bqv_details}->bind_param( 5, $bqv_data->{LENGTH} );
         $stmt->{insert_bqv_details}->bind_param( 6, $descr);
         $stmt->{insert_bqv_details}->execute();
      };

      if ($@) {
         die( "Cannot insert bqv details for '$entry_data->{AC}'.\n$@" );
      }

    } else {# entry is already in the DB -> update

      eval {
         $stmt->{update_bqv_details}->bind_param( 1, $entry_data->{SEQ_VERSION} );
         $stmt->{update_bqv_details}->bind_param( 2, $bqv_blob, {ora_type => ORA_BLOB} );
         $stmt->{update_bqv_details}->bind_param( 3, $cksum );
         $stmt->{update_bqv_details}->bind_param( 4, $bqv_data->{LENGTH} );
         $stmt->{update_bqv_details}->bind_param( 5, $descr );
         $stmt->{update_bqv_details}->bind_param( 6, $entry_data->{AC} );
         $stmt->{update_bqv_details}->execute();
      };

      if ($@) {

        die( "Cannot update bqv details for '$entry_data->{AC}'\n$@\n" );
      }
    }
  }
}


sub get_args {
   # 
   # 
   # 
   my ($progname) = $0 =~ m|([^/]+)$|;

   my %opts = ( -no_load_seq => 0,
                -no_load_bqv => 0,
                -no_parse    => 0,
                -no_report   => 0 );

   my $usage_opts = '['. join('] [', keys( %opts ) ) .']';
   my $USAGE = <<USAGE;

$progname <user/passw\@instance> <file name> -p <project number> [<extra putff args>] $usage_opts

   Loads (via putff) a flatfile containing BQVs:
     1) Strips all base quality lines from <file name> and stores them in the
          <file name>.bqv file
     2) Calls
        'putff <user/passw\@instance> <file name> -summary -p <project number> <extra putff flags>'
     3) Loads the corresponding BQVs for each succesfully loaded entry.
     4) Prints a report of the loading.

   You can inhibit some steps by using the -no_* options.

   Writes the files:
     <file name>.bqv              -> contains the BQVs entries
     <file name>.dat              -> contains the sequence entries
     <file name>.dat.summary.xml  -> contains the XML summary from putff
     <file name>.report           -> contains the loading report

USAGE

   my $login = shift(@ARGV);
   my $fname = shift(@ARGV);

   unless( @ARGV ) {

     print( STDERR $USAGE );
     exit;
   }

   my $extra_flags;

   foreach my $flag ( @ARGV ) {

     $flag = lc( $flag );

     if ( defined( $opts{$flag} ) ) {

       $opts{$flag} = 1;

     } else {

       $extra_flags .= " $flag";
     }
   }

   unless (-r($fname) ) {

     print( STDERR "ERROR: '$fname' does not exists or it is not readable by you.\n" );
     exit;
   }

   return ($login, $fname, $extra_flags, \%opts);
}

sub format_error {
  # 
  # 
  # 
  my ( $error_string ) = @_;

  use Text::Wrap qw(wrap);
  $Text::Wrap::columns = 97;

  my $formatted_error_string = join( '', wrap( "ERROR: ", "ERROR: ", "$error_string\n" ) );

  return $formatted_error_string;
}



