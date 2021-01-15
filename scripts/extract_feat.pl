#!/ebi/services/tools/bin/perl -w
#
use strict;

my ($file, $feat) = @ARGV;

my $USAGE = "USAGE:\n $0 <file_name> <feature>\nPrints AC for entries in <file_name> having the <feature> feature.\n\n";

if ( !$feat || !(-f($file)) ) {

	die($USAGE);
}

my @acs = extract($file, $feat);
$, = "\n";
print( @acs, );

sub extract {
	#
	#
	#
	
	my ($file, $feat) = @_;

	open(IN, "<$file") or die ("Can't open '$file' for reading.\n$!");

	my $ac;
	my $ac_re = qr/AC   (\w*);/;
	my $feat_re = qr/FT   $feat/;
	
	while(<IN>){

		if (m/$ac_re/) {
			
			$ac = $1;
		}

		if (m/$feat_re/) {

			print("$ac\n");

			while(<IN>) {
				last if $_ eq "//\n";
			}
		}
	}

	close(IN);
}

