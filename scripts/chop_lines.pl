#! /ebi/services/tools/bin/perl -w
# Splits long FT lines
# 7 jul 2003    F. Nardone
#
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

use strict;
use constant (MAXLEN => 80);


main ();

sub main {
  #
  #
  #

  my $file = $ARGV[0];

  unless ($file) {

    die "USAGE:\n".
        "  $prog <file name>\n".
        "  Split FT lines > ". MAXLEN ." chars on commas.\n".
        "  The resulting file is named <file name>_chop.\n";
  }

  my $fname_out =  $file . '_chop';

  open (IN, "<$file") or die ($!);
  open (OUT, ">$fname_out") or die ($!);

  while (my $line = <IN>) {

    chomp($line);

    if ( length( $line ) > MAXLEN ) {

      if (substr( $line,0 ,2) ne 'FT') {
        die("'$line' if too long and it is not a feature line");
      } else {
        $line = chop_line( $line );
      }
    }
    print( OUT $line ."\n" );
  }

  close (OUT);
  close (IN);
}

sub chop_line {
  #
  #
  #
  my ($line) = @_;
  my @lines;

  if (length($line) <= MAXLEN) {
    return $line;
  }

  my $pos;
  for ( $pos = MAXLEN - 1; ($pos > 0) && (substr($line, $pos, 1) ne ','); --$pos ) {
  }

  if ($pos <= 0) {

    die("'$line'\n is too big and it does not have commas.\n");

  } else {

    ++$pos;#so we put the comme in the first line
    push( @lines, substr($line, 0, $pos));
    my $remaining = "FT                   " . substr($line, $pos);

    eval {
      push (@lines, chop_line($remaining));
    };
    if ($@) {
      die("'$line'\n is too big and it does not have commas.\n");
    }

    return join("\n", @lines);
  }
}

