=head1 DESCRIPTION

This module provides general utility functions.

=cut

package Utils;
use strict;
use Exporter ();
use vars qw(@ISA @EXPORT_OK);

@ISA = qw (Exporter);
@EXPORT_OK = qw (my_open my_open_gz my_opendir my_die my_system my_rename
                 my_unlink printfile readfile);

=head1 PUBLIC FUNCTIONS

=cut

sub my_open {

=over 4

=item C<my_open>

USAGE:
       1) my $fh = my_open( "<blah" );
       or
       2) Utils::my_open( \*IN, "<$file_name" );

Opens a file and dies with an error message + call stack trace if it
fails.

=cut

  my( $fh, $fname );

  if ( !defined( $_[1] ) ) {

    $fname = $_[0];

  } else {

    ($fh, $fname) = @_;
  }

  unless ( open( $fh, $fname ) ) {
    my_die( "Can't open '$fname', $!" );
  }

  return $fh;
}

sub my_open_gz {


=item C<my_open_gz>

USAGE:
       1) my $fh = my_open_gz( "<blah.gz" );
       or
       2) Utils::my_open( \*IN, "<$file_name" );

Opens a file and dies with an error message + call stack trace if it
fails.

If the file is a *.gz file it opens a pipe from gunzip. i.e. you don't have to
uncompress it.

=cut

  my( $fh, $fname );

  if ( !defined( $_[1] ) ) {

    $fname = $_[0];

  } else {

    ($fh, $fname) = @_;
  }

  if ( $fname =~ m/\.gz$/ ) {

    if ( $fname =~ m/^>/ ) {# open for write ?

      my_die( "You cannot write to a .gz file.\n" );

    } else {

      $fname =~ s/^<*//;
      $fname = "gunzip -c '$fname' |";# opens a pipe from gunzip
    }
  }

  unless ( open( $fh, $fname ) ) {
    my_die( "Can't open '$fname', $!" );
  }

  return $fh;
}

sub my_opendir {


=item C<my_opendir>

USAGE:

    my $dh = my_open( '/path/to/the/dir' );

Opens a directory and dies with an error message + call stack trace if it
fails.

=cut

  my( $dirname ) = @_;

  my $dh;

  unless ( opendir( $dh, $dirname ) ) {
    my_die( "Can't open '$dirname', $!" );
  }

  return $dh;
}

sub my_rename {

=item C<my_rename>

USAGE:

    my_rename( $current_name, $new_name );

Rename the file called $current_name to $new_name or dies if it can't.
The usual caveats about the rename function apply.
(see perldoc -f rename).

=cut

  my( $current_name, $new_name ) = @_;

  unless ( rename( $current_name, $new_name ) ) {
    my_die( "Can't rename '$current_name' to '$new_name'\n$!" );
  }
}


sub my_unlink {

=item C<my_unlink>

USAGE:

    my_unlink( $file_name );

Calls unlink and dies if an error occours.

=cut

  my( @file_names ) = @_;

  foreach my $fname ( @file_names ) {
    unless ( unlink( $fname ) ) {
      my_die( "Can't unlink '$fname'\n$!" );
    }
  }
}


sub printfile {

=item C<printfile>

USAGE:

    printfile( $fname, $text );

Writes the string contained in $text to the file $file.

=cut

  my( $fname, $text ) = @_;

  my $fh = my_open( ">$fname" );
  print( $fh $text );
  close( $fh );
}


sub readfile {

=item C<readfile>

USAGE:

    my $text = readfile( $fname );

Returns the content of the file $file as a string.

=cut

  my( $fname ) = @_;

  my $text = '';

  my $fh = my_open( $fname );
  while( <$fh> ) {
    $text .= $_;
  }
  close( $fh );

  return $text
}


sub my_system {

=item C<my_system>

USAGE:
       Utils::my_system( $command );

Executes a system command and dies with an error message + call stack trace
if it fails.

=cut

  my( $cmd ) = @_;

  system( "$cmd" ) and my_die( "Can't execute '$cmd', $!" );
}


sub my_die {

=item C<my_die>

USAGE:
       Utils::my_die( "That's wrong!" );

Prints an error message + call stack trace to STDERR and then dies.

=cut

  my( $msg ) = @_;

  print( STDERR "ERROR: $msg\n" );
  print( STDERR "------\n" );

  my ( $filename, $line, $subroutine, $level ) = ('', '', '', '');
  for ( $level = 0; caller($level); ++$level ) {
    ( $filename, $line, $subroutine ) = (caller( $level ))[1, 2, 3];
      print( STDERR " CALL: $subroutine at $filename, line $line\n" );
  }

  die( "\n" );
}

1;

=back
