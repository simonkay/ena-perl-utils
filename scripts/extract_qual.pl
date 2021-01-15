#!/ebi/services/tools/bin/perl -w
#
use strict;

my ($file, $qual) = @ARGV;

my $USAGE = "USAGE:\n $0 <file_name> <qualifier>\nPrints AC for entries in <file_name> having the /<qualifier> qualifier.\n\n";

if ( !$qual || !(-f($file)) ) {

	die($USAGE);
}

my @acs = extract($file, $qual);
$, = "\n";
print( @acs, );

sub extract {
	#
	#
	#
	
	my ($file, $qual) = @_;

	open(IN, "<$file") or die ("Can't open '$file' for reading.\n$!");

	my $ac;
	my $ac_re = qr/AC   (\w*);/;
	my $qual_re = qr/FT                   \/$qual/;
	
	while(<IN>){

		if (m/$ac_re/) {
			
			$ac = $1;
		}

		if (m/$qual_re/) {

			print("$ac\n");

			while(<IN>) {
				last if $_ eq "//\n";
			}
		}
	}

	close(IN);
}

