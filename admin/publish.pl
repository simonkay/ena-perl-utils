#!/ebi/services/tools/bin/perl -w
#
#
#
#
my ($pwd, $prog);

BEGIN
{
  if ($0 =~ m|^(.*/)([^/]*)$|){
    $pwd = $1;
    $prog = $2;

  }else{
    $pwd = './';
    $prog = $0;
  }
}

use strict;
use File::Copy;

my $LIBDIR = "${pwd}../perllib";
my $DESTLIB = '/ebi/services/tools/seqdb/perllib';

my $PROGDIR = "${pwd}../scripts";
my $DESTPROG = '/ebi/services/tools/seqdb/bin';


main ();


sub main {
  #
  #
  #

  my @programs = @ARGV;

  foreach (@programs) {
    s|^.*/([^/]*)$|$1|;# get only the file name
    print "$_\n";
  }

  unless (@programs){
    die (
"USAGE: $0 <program1> <program2> ...
    Copyes all files associated with the named programs to their 
    final destinations.
    To publish all do: '$0 ../scripts/*' \n");
  }

  my (@libs, $lib_name, $program_name);
  foreach $program_name (@programs) {

    @libs = get_libs ($program_name);

    print (STDERR "Program: $program_name\n");
    copy ("$PROGDIR/$program_name", "$DESTPROG/$program_name") or die ("Cannot copy $program_name\n$!\n");
    chmod (0555, "$DESTPROG/$program_name", );

    print (STDERR "Libraries:\n");
    foreach (@libs) {
      print (STDERR " $_\n");
      copy ("$LIBDIR/$_", "$DESTLIB/$_") or die ("Cannot copy $_\n$!\n");
      chmod (0444, "$DESTLIB/$_");
    }
  }

  print ("Done!\n\n");
}



sub get_libs {
  # RETURN an array containing all libs 'use'd by the program <$program>
  #
  #

  my ($program) = @_;

  open (IN, "<$PROGDIR/$program") or die ("Cannot open $PROGDIR/$program\n$!\n");

  my @libs;

  while (<IN>) {
    if ( m/^ *use +(\w+)/ && (-f "$LIBDIR/$1.pm") ) {
      push (@libs, "$1.pm");
    }
  }

  close (IN);

  return @libs;
}
