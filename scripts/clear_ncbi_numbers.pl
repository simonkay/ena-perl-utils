#!/ebi/services/tools/bin/perl -w

my ($pwd, $prog);

BEGIN
{
   my $fname = __FILE__;
   if ($fname =~ m"(^|^.*/)([^/]+)$") {#"
      $pwd = $1;
      $prog = $2;

   }else{
      die ("ERROR: Cannot figure out where I am!\n");
   }
}

use strict;
use lib "${pwd}../perllib";

main();

sub main {
  #
  #
  #
  my $fname = $ARGV[0];
  
  unless ( defined( $fname ) ) {
    die( "USAGE:\n" .
         " $0 <file name>\n" .
         "Strips nucleotide numbers from an NCBI faltfile.\n" .
         "The resulting file is saved in <file name>.clear.\n\n" );
  }
     
  open( IN, "<$fname" ) or die( $! );
  open( OUT, ">$fname.clear" ) or die( $! );

  my $flag = 0;

  while ( my $line = <IN> ) {

    my $linetype = substr( $line, 0, 6 );

    if ( $linetype eq "//\n" ) {
      $flag = 0;
    }

    if ( $flag ) {
      substr( $line, 0, 9 ) = '         ';
    }

    print( OUT $line );

    if ( $linetype eq 'ORIGIN' ) {
      $flag = 1;
    
    }
  }

  close( OUT );
  close( IN );
}

