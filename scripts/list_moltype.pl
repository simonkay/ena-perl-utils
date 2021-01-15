#!/ebi/services/tools/bin/perl -w
#
#

use strict;


main ();

sub main {
	#
	#
	#

	unless (@ARGV) {
	
		die(<<USAGE);
$0 <file1> <file2> ...\n
  For each file creates two files:
   <file1>.single    : contains a list of accnos and /mol_type values
                        for entries having a single /mol_type value.
                        
   <file1>.multiple  : contains the whole flatfile of entries having
                        more than 1 /mol_type.

USAGE

}

	foreach my $fname (@ARGV) {
		open (IN, "<$fname") or
			print(STDERR "Can't open '$fname' for reading.\n$!\n\n");

		open (SINGLE_LIST, ">$fname.single") or
			print(STDERR "Can't open '$fname.single' for writing.\n$!\n");

		open (MULTI_LIST, ">$fname.multiple") or
			print(STDERR "Can't open '$fname.multiple' for writing.\n$!\n");

		my ($ac, @mol_types);

      my $start = 0;
		while (<IN>) {

			if (m/ACCESSION   (\w+)/) {
			
				$ac = $1;

            while (<IN>) {

               if (m| +/mol_type="(.+)"|) {

                  push (@mol_types, $1);
               
               } elsif (m|^//|) {

                  if (0 > $#mol_types) {# no mol_type
                     ;# do nothing

                  } elsif (0 == $#mol_types) {# one mol_type

                     print(SINGLE_LIST "$ac $mol_types[0]\n");

                  } else {# more than one mol_type

                     print_whole_entry($start);
                  }
                  
                  @mol_types = ();#empty the mol_types array
                  $start = tell(IN);# remember the beginning of this entry
                  last;
               }

            }
			}
		}

		close(MULTI_LIST);
		close(SINGLE_LIST);
		close(IN);
	}
}


sub print_whole_entry {
   #
   #
   #
   my ($start) = @_;

   seek(IN, $start, 0);

   while (<IN>) {

      print (MULTI_LIST $_);
      last if (m|^//|);
   }
}

