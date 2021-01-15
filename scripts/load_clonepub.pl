#!/ebi/services/tools/bin/perl -w
# 
# 

my ($pwd, $prog);

BEGIN
{
   my $fname = __FILE__;
   if ($fname =~ m"(^|^.*/)([^/]+)$") {#" this quote is just to fool syntax colorizers
      $pwd = $1;
      $prog = $2;
   }else{
      die ("Cannot figure out where I am!\n");
   }
}

use strict;
use lib "${pwd}../perllib";

use FFutils;
use Putff;
use Utils qw(my_open);

my ($conn, $fname, $putff_args) = get_args( @ARGV );
main( $conn, $fname, $putff_args );


sub main {

  my ( $conn, $fname, $putff_args ) = @_;
  
  my ( $model_fname, $rest_fname ) = FFutils::extract_first_entry( $fname );
 
  my ( $same_fname, $other_fname ) = FFutils::grep_publication( $model_fname, $rest_fname );

  print STDERR "LOG: Loading the sample entry.\n";
  my $model_comm = "$conn $model_fname $putff_args -summary";
  print STDERR "$model_comm\n";
  Putff::putff( $model_comm, "$model_fname.err" ) and 
    die( "Could not load sample entry with:\n '$model_comm'.\n$!\n" );

  my ( $loaded_model, $failed_model ) = Putff::get_accnos( $model_fname );

  my $model_accno;
  if ( defined( $loaded_model->[0] ) ) {

    $model_accno = $loaded_model->[0];
 
  } else {

    die( "Could not load '$model_fname'.\n$!\n" );
  }
  
  my $failed_total = 0;

  if ( -s( $same_fname ) ) {
  
    print STDERR "LOG: Loading with -clonepub.\n";
    my $same_comm = "$conn $same_fname $putff_args -clonepub $model_accno";
    print $same_comm."\n";
    Putff::putff( $same_comm, "$same_fname.err" ) and
    die( "Could not load with:\n '$same_comm'.\n" );

    my ( $loaded_same, $failed_same ) = Putff::parse_err_file( "$same_fname.err" );
    my $failed_total = $failed_same;
    
  } else {

    print STDERR "WARNING: No matching publications found.\n";
  }
  
  if ( -s( $other_fname ) > 0 ) {

    print STDERR "LOG: Loading entries without clonepub.\n";
    my $other_comm = "$conn $other_fname $putff_args";
    print STDERR "$other_comm\n";
    Putff::putff( $other_comm, "$other_fname.err" ) and
      die( "Could not load with:\n '$other_comm'.\n" );
  
    my ( $loaded_other, $failed_other ) = Putff::parse_err_file( "$other_fname.err" );
    $failed_total += $failed_other;
  }
  
  return $failed_total;
}


sub get_args {

  my ( $conn, $fname, @putff_args ) = @ARGV;

  unless ( defined( $fname ) ) {

    die( "\nload_clonepub <user/passw\@instance> <file name> [ <putff arguments> ]\n".
         "  Loads a file trying to use the -clonepub option in putff.\n".
         "\n".
         "  - Produces files:\n".
         "    <file name>.first and <file name>.remaining\n".
         "    containing resp. the forst entry and all the others.\n".
         "    <file name>.remaining.same and <file name>.remaining.other\n".
         "    containing resp. all entries sharing the same publication as the first\n".
         "    entry in the file and all the other entries (RP and RC lines are ignored).\n".
         "  - Loads <file name>.first.\n".
         "  - Loads <file name>.remining.same with the -clonepub option.\n".
         "  - Loads <file name>.remining.other without the -clonepub option.\n".
         "\n".
         "  The sdterr + stdout for each file loading will be in the corresponding\n".
         "   *.err file\n".
         "  Extra putff flags can be added, they will be used in all the loading stages.\n" );
  }

  my $putff_args_string = join( ' ', @putff_args );

  return ($conn, $fname, $putff_args_string);
}
