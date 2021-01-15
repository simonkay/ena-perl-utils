#!/ebi/services/tools/bin/perl -w
# This script gives information about the logical structure of Oracle
#  database tables.
#
# USAGE: describe.pl <user/password@database> [<owner>.]<table name> [-<options>]
#


my ($pwd, $prog);

BEGIN
{
  my $fname = __FILE__;
  if ($fname =~ m"(^|^.*/)([^/]+)$") {
    $pwd = $1;
    $prog = $2;

  }else{
    die ("Cannot figure out where I am!");
  }
}

use strict;
use lib "${pwd}../perllib";
use ASCII_Table;
use DBI;
use Oracle_Table_Info;


main ();



sub main {
  # Do the business
  #
  #

  my ($connection, $owner, $table_name, %OPTIONS) = read_command_line ();

  my $dbh = DBI->connect ('dbi:Oracle:', "$connection", '',
                          {PrintError => 0, AutoCommit => 0, RaiseError => 1});

  print ("Table $table_name:\n");
  
  my $table_comment = get_table_comment ($dbh, $owner, $table_name);

  if ( $table_comment ) {
    print ("$table_comment\n");
  }

  # columns info
  my $table_descr_arr = get_table_description ($dbh, $owner, $table_name, %OPTIONS);

  if ( $#$table_descr_arr > 0 ) {

    print ("\nColumns:\n");
    print_table ($table_descr_arr);

    unless ($OPTIONS{primary}){
      # primary key

      print ("\nPrimary Key constraint:\n");

      my $primary_key_arr = get_table_primary_key ($dbh, $owner, $table_name);

      print_table ($primary_key_arr);
    }

    unless ($OPTIONS{foreign}){
      
      # foreign keys OUT
      print ("\nForeign keys:\n");

      my $foreign_key_arr_OUT = get_table_foreign_key_OUT ($dbh, $owner, $table_name);

      print_table ($foreign_key_arr_OUT);

      # foreign keys IN
      print ("\nReferences by foreign keys:\n");

      my $foreign_key_arr_IN = get_table_foreign_key_IN ($dbh, $owner, $table_name);

      print_table ($foreign_key_arr_IN);
    }

    unless ($OPTIONS{check}){
      # check constraints

      print ("\nCheck constraints:\n");

      my $check_constraints_arr = get_table_check_constraints ($dbh, $owner, $table_name);

      print_table ($check_constraints_arr);
    }

    unless ($OPTIONS{privileges}){
      # access privileges

      print ("\nAccess privileges:\n");

      my $privileges_arr = get_table_privileges ($dbh, $table_name);

      print_table ($privileges_arr);
    }

    unless ($OPTIONS{indexes}){
      # indexes
      
      print ("\nIndexes:\n");

      my $indexes_arr = get_table_indexes ($dbh, $owner, $table_name);

      print_table ($indexes_arr);
    }


  }else{
    print ("Does not exist or is not visible by you.\n");

  }

  $dbh->disconnect ();
}



sub print_table {
  # Print the content of a 2D array
  # needs the ASCII_Table module
  #

  my ($table_arr) = @_;

  if ( $#$table_arr > 0 ) {
    print ( ascii_table($table_arr) );

  }else{
    print ("None\n");
  }
}


sub read_command_line {
  # Does the obvious
  # RETURN a list of <connection> <owner> <table name> %OPT
  # %OPT is a hash containing all set options
  # <owner> is the empty string if the owner is not specified 
  #  on the command line.
  #

  my $USAGE = <<END_STR
$prog <user/passw\@database> [<owner>.]<table_name> [-cpfkgi]
     options:
       c   do not print column comments
       p   do not print primary key
       f   do not print foreign keys
       k   do not print check constraints
       g   do not print grant privileges
       i   do not print indexes information

END_STR
;
  if ($#ARGV < 1 || $#ARGV > 2) {
    die ($USAGE);
  }

  my %OPT;
  if (defined ($ARGV[2]) ) {
    my $options = $ARGV[2];

    unless ( $options =~ s/^-// ){
      die ($USAGE);
    }

    if ( $options =~ m/c/) {
      $OPT{comments} = 1;
    }
    if ( $options =~ m/p/) {
      $OPT{primary} = 1;
    }
    if ( $options =~ m/f/) {
      $OPT{foreign} = 1;
    }
    if ( $options =~ m/k/) {
      $OPT{check} = 1;
    }
    if ( $options =~ m/g/) {
      $OPT{privileges} = 1;
    }
    if ( $options =~ m/i/) {
      $OPT{indexes} = 1;
    }
  }

  my $table_name = uc ($ARGV[1]);

  if ($table_name =~ m/@/) {
    die ("\nYou cannot use the notation '<owner>.<table name>@<database>'\nUSAGE:\n$USAGE");
  }

  my $owner = '';
  if ($table_name =~ s/^(\w+)\.(\w+)$/$2/) {
    $owner = $1;

  }

  return ($ARGV[0], $owner, $table_name, %OPT);
}

