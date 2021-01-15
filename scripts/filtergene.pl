#!/ebi/services/tools/bin/perl -w

#--------------------------------------------------------------------------------
# filters gene qualifier out of given file
# 29-APR-2003  ckanz
# 16-MAY-2003  nardone uses the Ncbi.pm module
#
#--------------------------------------------------------------------------------

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
use Ncbi;

if ($#ARGV > 1 || $#ARGV < 0) {

	die("USAGE: $0 <file_in> [<file_out>]\n" .
	    "       Removes 'gene' qualifiers form <file_in> and writes the result to <file_out>.\n" .
		  "       If <file_out> is not specified it does in-place editing of <file_in>.\n");
}

my ( $numseq ) = Ncbi::filter_gene_qualifiers ( @ARGV );
print " $numseq sequences\n";

