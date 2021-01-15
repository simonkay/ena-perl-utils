#! /ebi/services/tools/bin/perl -w
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
      die ("Cannot figure out where I am!\n");
   }
}

use strict;
use lib "$pwd../perllib";
require FFutils;
require Utils;

main ();

sub main {
  #
  #
  #
  my( $file_list ) = read_args();

  foreach my $fname ( @$file_list ) {
    my $format = FFutils::guess_format( $fname );
    print( "$fname, $format format.\n" );
    check_genes( $fname, $format );
    print( "\n");
  }
}
  
sub check_genes {
  #
  #
  #
  my( $fname, $format ) = @_;

  open( IN, "<$fname" ) or die( "ERROR: can't open '<$fname'\n$!" );

  my( $ac_re, $genequal_re );
  my $genename_re = qr'^[a-z]{3}[A-Z][0-9]*$';
  my $ignore_re = qr't|rRNA';
  
  if ( $format eq 'ncbi' ) {
    $ac_re = qr'^ACCESSION   ([A-Z]+\d+)';
    $genequal_re = qr'^                     /gene="(.*)"';
    
  } else {
    $ac_re = qr'^AC   ([A-Z]+\d+);';
    $genequal_re = qr'^FT                   /gene="(.*)"';
  }
  
  my( $ac, @genes, %genes, $in_cds );
  %genes = ();

  while ( <IN> ) {

    if ( m/$ac_re/ && !$ac ) {

      $ac = $1;
      @genes = ();
      %genes = ();
    }

    if ( m/$genequal_re/ && !m/$ignore_re/ ) {
      my $gene_name = $1;

      if ( $gene_name !~ m/$genename_re/ ) {
        print( "Bad gene name for AC $ac\n".
          "$_" );

      } else {
        $genes{$gene_name}++;
      }
    }

    if ( m|^//| ) {

      while ( my( $gene, $instances ) = each( %genes ) ) {
        if ( $instances > 1 ) {
          print( "/gene=\"$gene\" appears $instances times in AC $ac.\n" );
        }
      }
      $ac = '';
    }
  }

  close( IN );
}


sub read_args {
  #
  #
  #

  my $USAGE = (<<END_STR);
$prog <file name1> <file name2> <file name3> ... 
   Check gene names in each file (which can be EMBL or NCBI format).
   Prints a message if:
     i) Any /gene contains a value not matching ^[a-z]{3}[A-Z][0-9]*\$.
    ii) Two or more /gene have the same value in the same entry.
    
   Gene names matching t|rRNA are ignored both for (i) and (ii).

END_STR


  my( $fnames ) = \@ARGV;

  unless ( scalar( @$fnames ) ){
    die( $USAGE );
  }

  return $fnames;
}


