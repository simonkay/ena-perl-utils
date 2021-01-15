#!/ebi/services/tools/bin/perl -w
#
#     updpid <uname/passw@instance> <old_acc> <new_acc>
#
# checks:
#     old_acc' entry_status = 'D' deleted  (fatal error if not)
#     new_acc' entry_status != 'D' deleted  (fatal error if yes)
#
#
#
#



my ($pwd, $prog);

BEGIN
{
   my $fname = __FILE__;
   if ($fname =~ m"(^|^.*/)([^/]+)$") {
      $pwd = $1;
      $prog = $2;

   }else{
      die ("Cannot figure out where I am!\n");
   }
}

use strict;
use lib "${pwd}../perllib";
use DBI;

main ();


sub main {
   # Do the business
   #
   #


   my ($connection, $old_acc, $new_acc, $no_update, $verbose) = read_command_line ();

   my ($dbh, %cursors) = connect_to_db ($connection);

   my $sql = "begin remark.put('Fixed pid version updpid.pl'); end;";
   $dbh->do($sql);



   my ($old_pids_data, $new_pids_data);
   eval {
      die_if_not_deleted ($old_acc, $cursors{get_entry_status});
      die_if_deleted     ($new_acc, $cursors{get_entry_status});
   
      $old_pids_data = get_pids_info($old_acc, $cursors{get_pids_info});
      $new_pids_data = get_pids_info($new_acc, $cursors{get_pids_info});
   };
   if ($@) {
      close_db ($dbh, %cursors);
      die ($@);
   }

   my $msg;
   while ( my ($pid, $info_new_hr) = each (%$new_pids_data) ) {
      
      if ( defined (my $info_old_hr = $$old_pids_data{$pid}) ) {
         
         $msg = "PID: $pid\n";

         if ( $$info_new_hr{version} > $$info_old_hr{version} ) {
            # new version > old version => do nothing
            $msg .= " new version: $$info_new_hr{version}\n old version: $$info_old_hr{version}\n DO NOTHING\n";
         
         } elsif ( $$info_new_hr{version} < $$info_old_hr{version} ) {
            # new version < old version => incr new version
            $msg .= " new version: $$info_new_hr{version}\n old version: $$info_old_hr{version}\n";
            $msg .= " NEW VERSION = " . (1 + $$info_old_hr{version}) . "\n";
            
            print STDERR "$msg\n" if ($verbose);

            unless ($no_update) {

               eval {
                  update_version ( $$info_new_hr{seqid},
                                   (1 + $$info_old_hr{version}),
                                   $cursors{update_version});
               };
               if ($@) {
                  $dbh->rollback();
                  print (STDERR "Rollback\n");
                  close_db($dbh, %cursors);
                  die($@);
               }
            }

         } else { # versions are equal here
            # new version == old version => check chksum
            $msg .= " new version: $$info_new_hr{version}\n old version: $$info_old_hr{version}\n";
            $msg .= " same versions\n";

            if ( $$info_new_hr{chksum} != $$info_old_hr{chksum} ) {
               # checksum differ => increment new version
               $msg .= " checksums differ\n NEW VERSION = " . (1 + $$info_new_hr{version}) . "\n";
               
               print STDERR "$msg\n" if ($verbose);

               unless ($no_update) {
                  eval {
                     update_version ( $$info_new_hr{seqid},
                                      (1 + $$info_new_hr{version}),
                                      $cursors{update_version});
                  };
                  if ($@) {
                     $dbh->rollback();
                     print (STDERR "Rollback\n");
                     close_db($dbh, %cursors);
                     die($@);
                  }
               }
            
            } else {
               # same checksum => same version
               $msg .= " checksums are the same\n DO NOTHING\n";
            
            }

         }

      }
   }

   $dbh->commit();
   close_db ($dbh, %cursors);
}


sub update_version {
   my ($seqid, $version_number, $cursor) = @_;

   $cursor->bind_param(1, $version_number);
   $cursor->bind_param(2, $seqid);
   $cursor->execute();
}


sub close_db {

   my ($dbh, %cursors) = @_;

   
   # close all cursors
   foreach my $cursor ( values(%cursors) ) {
     $cursor->finish();
   }
   
   $dbh->disconnect ();
}



sub get_pids_info {

   my ($accno, $cursor) = @_;

   $cursor->bind_param (1, $accno);

   $cursor->execute();

   my %ret;

   while ( my $row_hr = $cursor->fetchrow_hashref() ) {
      
      $ret{$$row_hr{pid}} = $row_hr;
   }

   return (\%ret);
}



sub die_if_not_deleted {


   my ($accno, $cursor) = @_;

   $cursor->bind_param(1, $accno);
   $cursor->execute();

   my ($entry_status) = $cursor->fetchrow_array();

   if ( !defined($entry_status) ) {
      die ("Accession number '$accno' does not exists in the database.\n");

   } elsif ( $entry_status ne 'D' ) {
      die ("The old accession number must refer to a deleted entry.\n'$accno' has entry status '$entry_status'\n");
   }

}



sub die_if_deleted {


   my ($accno, $cursor) = @_;

   $cursor->bind_param(1, $accno);
   $cursor->execute();

   my ($entry_status) = $cursor->fetchrow_array();

   if ( !defined($entry_status) ) {
      die ("Accession number '$accno' does not exists in the database.\n");

   } elsif ( $entry_status eq 'D' ) {
      die ("The new accession number must refer to a live entry.\n'$accno' has entry status '$entry_status'\n");
   }

}



sub connect_to_db {

   my ($connection) = @_;


   my $dbh = DBI->connect ('dbi:Oracle:', "$connection", '',
                           {PrintError => 1, AutoCommit => 0, RaiseError => 1});


   my ($sql, %cursors);

   $sql = <<SQL;
SELECT
       protein_id "pid",
       protein_id_version "version",
       chksum "chksum",
       seqlen "seqlen",
       seqid "seqid",
       pseudo

  FROM
       embl\$protein_ids

 WHERE
       primaryacc# = ?
SQL

   $cursors{get_pids_info} = $dbh->prepare($sql);


   $sql = <<SQL;
SELECT
       entry_status
       
  FROM
       dbentry
       
 WHERE
       primaryacc# = ?
SQL

   $cursors{get_entry_status} = $dbh->prepare($sql);

   $sql = <<SQL;
UPDATE
       bioseq
   
   SET
       version = ?
 
 WHERE
       seqid = ?
SQL

   $cursors{update_version} = $dbh->prepare($sql);


   return ($dbh, %cursors);
}



sub read_command_line {
   # Does the obvious
   # RETURNS: ($connection, $old_acc, $new_acc, $no_update, $verbose)
   #

   use Getopt::Long;
   Getopt::Long::Configure ("bundling_override");

  my $USAGE = (<<END_STR);
$prog <user/passw\@database> <old_acc#> <new_acc#> [-noupdate] [-verbose]
     Update protein ID version numbers in the database.
     <old_acc#> is a secondary accession number of <new_acc#>.
     -noupdate does not update pids in the database,
               turns -verbose on.
     -verbose  prints a message for each pid to be modified.

END_STR

   unless ($ARGV[2]) {
      die ($USAGE);
   }

   my $connection = shift(@ARGV);
   my $old_acc = uc ( shift(@ARGV) );
   my $new_acc = uc ( shift(@ARGV) );

   my ($verbose, $no_update) = ('', '');

   unless (GetOptions('verbose'  => \$verbose,
                      'noupdate' => \$no_update) ) {
      die $USAGE;
   }

   if ($no_update) {
      $verbose = 1;
   }

   return ($connection, $old_acc, $new_acc, $no_update, $verbose);
}
