#!/ebi/services/tools/bin/perl -w
#
use strict;

my ($file, $de_word) = @ARGV;

my $USAGE = "USAGE:\n $0 <file_name> <word>\nPrints AC for entries in <file_name> having <word> in the DE line.\n\n";

if ( !$de_word || !(-f($file)) ) {

	die($USAGE);
}

extract($file, $de_word);

sub extract {
	#
	#
	#
	
	my ($file, $de_word) = @_;

	open(IN, "<$file") or die ("Can't open '$file' for reading.\n$!");

	my $ac;
	my $ac_re = qr/AC   (\w*);/;
	my $de_word_re = qr/DE  .*$de_word/;
	
	while(<IN>){

		if (m/$ac_re/) {
			
			$ac = $1;
		}

		if (m/$de_word_re/) {

			print("$ac\n");

			while(<IN>) {
				last if $_ eq "//\n";
			}
		}
	}

	close(IN);
}

