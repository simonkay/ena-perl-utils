#!/ebi/services/tools/bin/perl -w
#
#

use strict;

unless (@ARGV) {

   die(<<USAGE);
$0 <file list>

Reads the list of files and for each one writes a <file name>.filtered file
 containing all entries from the original file having a '/segment' or '/locus_tag'
 qualifier. 

USAGE
}



main ();

sub main {
   #
   #
   #

   foreach my $file (@ARGV) {

      open (IN, "<$file") or
         die ("Can't open '$file' for reading\n$!\n");

      open (OUT, ">$file.filtered") or
         die ("Can't open '$file' for reading\n$!\n");

      my ($first_part, $line_type);
      while (<IN>) {
   
         $first_part .= $_;

         if (m|^ {21}/segment| ||
             m|^ {21}/locus_tag|) {

            write_entry(\*OUT, \*IN, $first_part);
            $first_part = '';

         } elsif (m|^BASE COUNT|) {

            skip_entry(\*IN);
            $first_part = '';
         }
      }

      close(OUT);
      close(IN);
   }
}



sub write_entry {
	# prints into $out_fh the content of $first_lines and the content of
	#  $in_fh until a '//' line is met
	#
	my ($out_fh, $in_fh, $first_lines) = @_;

	print ($out_fh $first_lines);
	
	while (<$in_fh>) {

		print ($out_fh $_);
		
		last if (m|^//|);
	}
}



sub skip_entry {
	#
	#
	#
	my ($in_fh) = @_;

	while (<$in_fh>) {

		last if (m|^//|);
	}
}

