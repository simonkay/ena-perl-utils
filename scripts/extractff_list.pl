#!/ebi/services/tools/bin/perl
#
# Retrieves a list of entries from a multiple entry flatfile.
#

use warnings;
use strict;

use Utils;
use FFutils qw(guess_format get_ac_re);

main ();

sub main {
  #
  #
  #

  my ($flatfile, $list_file) = read_cmd_line();

  my $format = guess_format( $flatfile );
  
  my $AC_re = get_ac_re( $format );
  
  open(IN_DATA, $flatfile) or die( "Cannot open $flatfile for reading.\n$!\n" );

  my $index_hr = index_file( \*IN_DATA, $AC_re );
  open(IN_LIST, $list_file) or die($!);

  while (<IN_LIST>) {

    chomp;
    print_entry(\*IN_DATA, $_, $index_hr);
  }

  close(IN_LIST);
  close(IN_DATA);
}


sub index_file {
  #
  #
  #
  my ($IN, $AC_re) = @_;
  my %index;
  my $secondary = 0;# flag for AC type
  my $pos = 0;

  while (<$IN>) {

    if (!$secondary && m/$AC_re/) {

      $index{$1} = $pos;
      $secondary = 1;# all ACs from now are seconadries

    } if ($_ eq "//\n") {

      $pos = tell($IN);
      $secondary = 0;# all ACs from now are NOT seconadries
    }
  }

  return \%index;
}


sub print_entry {
  #
  #
  #

  my($IN, $ac, $index_hr) = @_;

  my $pos  = $index_hr->{$ac};
  if ( defined( $pos ) ) {

    seek($IN, $pos, 0) or die ("ERROR: could not seek to $pos\n$!\n ");

    while(<$IN>) {

      print;
      last if ($_ eq "//\n");
    }

  } else {

    print STDERR "WARNING: '$ac' not found\n";
  }

}



sub read_cmd_line {
  #
  #
  #

  my($flatfile, $list_file) = @ARGV;

  unless($list_file) {

    die("USAGE:
      $0 <data file> <list file>
      extract all entries in <data file> (EMBL or NCBI format) with AC contained in <list file>\n");
  }

  unless(-r $flatfile && -r $list_file) {

    die("One or both of $flatfile and $list_file are not readable.\n");
  }

  return ($flatfile, $list_file);
}
