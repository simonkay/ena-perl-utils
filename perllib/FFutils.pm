package FFutils;

=head1 DESCRIPTION

This module provides functions which facilitate the handling of flat files.
No function is exported by default.

=cut


my ($pwd, $prog);

BEGIN
{
  my $fname = __FILE__;
  if ($fname =~ m"(^|^.*/)([^/]+)$") {#" 
    $pwd = $1;
    $prog = $2;

  }else{
    die ("Cannot figure out where I am!\n");
  }
}
use lib "${pwd}../perllib";

use strict;
use Utils qw(my_open my_die);


sub guess_format {

=head1 PUBLIC FUNCTIONS

=over 4

=item C<guess_format>

USAGE:

=for html
<pre>

       $format = FFutils::guess_format( $fname );

=for html
</pre>

Returns 'ncbi' or 'embl' depending on the format of the file.
dies if no 'LOCUS' or 'ID' line is met within the first 100 lines of the file.

=cut

  my( $fname ) = @_;
  my( $format );
  
  my_open( \*IN, "<$fname" );

  while ( <IN> ) {
    if ( m/^LOCUS/ ) {
      $format = 'ncbi';
      last;

    } elsif ( m/^ID   / ) {
      $format = 'embl';
      last;
    }

    if ( $. > 100 ) {
      last;
    }
  }
  close( IN );

  unless( $format ) {
    my_die( "ERROR: $fname does not appear to be a sequence file.\n" );
  }

  return $format;
}



sub grep_publication {

  my ($model_file, $file) = @_;

  my $format = guess_format( $file );
  my( $pub_begin_re, $pub_end_re, $pub_exclude_re );

  if ( $format eq 'ncbi' ) {
    $pub_begin_re = qr'^(REFERENCE   \d+)';
    $pub_end_re = qr'^\S';
    $pub_exclude_re = qr'^you\'ll never match this$'

  } else {
    $pub_begin_re = qr'^(RN   \[\d+\])';
    $pub_end_re = qr'^(?:[^R]|RN)';
    $pub_exclude_re = qr'^(?:RP|RC)';
  }
  
  my $model_pub = get_model_pub( $model_file, $pub_begin_re, $pub_end_re, $pub_exclude_re );

  my ( $file_same, $file_others ) = ( "$file.same", "$file.others" );
  my_open( \*SAME_PUB, ">$file_same" );
  my_open( \*OTHERS, ">$file_others" );
  my_open( \*IN, "<$file" );

  my $grab_lines = 0;
  my( $entry_begin, $pub ) = ( 0, '' );
  while ( <IN> ) {

    if ( m/$pub_begin_re/ ) {
      $pub .= "$1\n";
      $grab_lines = 1;

    } elsif ( $grab_lines and m/$pub_end_re/ and !m/$pub_begin_re/ ) {

      $grab_lines = 0;

      my $fh;

      if ( $pub eq $model_pub ) {
        $fh = \*SAME_PUB;

      } else {
        $fh = \*OTHERS;
      }

      seek( IN, $entry_begin, 0 );
      while ( <IN> ) {
        print( $fh $_ );
        last if ( m|^//| );
      }

      $entry_begin = tell( IN );

      $pub = '';
      
    } elsif ( $grab_lines and !m/$pub_exclude_re/ ) {

      $pub .= $_;
    }
  }

  close( IN );
  close( OTHERS );
  close( SAME_PUB );

  return ( $file_same, $file_others );
}


sub get_model_pub {

  my( $file, $pub_begin_re, $pub_end_re, $pub_exclude_re ) = @_;

  my_open( \*IN, "<$file" );

  my $grab_lines = 0;
  my $model_pub = '';

  while ( <IN> ){

    if ( m/$pub_begin_re/ ) {

      $model_pub .= "$1\n";
      $grab_lines = 1;

    } elsif ( $grab_lines && m/$pub_end_re/ && !m/$pub_begin_re/ ) {
      last;
      
    } else {
      $model_pub .= $_ if ($grab_lines && !m/$pub_exclude_re/);
    }
  }
  close( IN );

  unless ( $model_pub ) {
    die( "ERROR: no publication found in $file.\n" );
  }

  return $model_pub;
}


sub extract_first_entry {

  my ($fname) = @_;

  my ( $fname_first, $fname_remaining ) = ( "$fname.first", "$fname.remaining" );
  my $fh_in = my_open( "<$fname" );
  my $fh_out_first = my_open( ">$fname_first" );
  my $fh_out_remaining = my_open( ">$fname_remaining" );

  my $fh_out_current = $fh_out_first;
  
  while( <$fh_in> ) {

    print $fh_out_current $_;
    
    if ( m|^//| ) {
      $fh_out_current = $fh_out_remaining;
    }
  }
  
  close( $fh_out_remaining );
  close( $fh_out_first );
  close( $fh_in );

  return ($fname_first, $fname_remaining);
}


1;

=back

