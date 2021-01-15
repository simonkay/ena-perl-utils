package Ncbi;

use strict;
use TempFiles;

=head1 DESCRIPTION

This module provides functions to handle NCBI flat files.

=head1 PUBLIC FUNCTIONS

=over
=cut

=item C<filter_gene_qualifiers>

  $numseq = filter_gene_qualifiers($fname_in, $fname_out);
  $numseq = filter_gene_qualifiers($fname);

Eliminates 'gene' lines from the feature table of file I<$fname_in> which
 must be an NCBI flat file.

The filtered file is written to I<$fname_out>, if it is called with only 
 one argument the file is edited 'in place'.

Returns: the number of entries in the file.

=cut

sub filter_gene_qualifiers {

	my $GENE_BEGIN = '     gene            ';
	my $GENE_END   = '                     ',
	
	my ($fname_in, $fname_out) = @_;

	
	my $real_fname_out;
	if (defined($fname_out) && ($fname_out ne $fname_in)) {

    $real_fname_out = $fname_out;

	} else {

		$real_fname_out = temp_file('.', 'filter_gene_qual');
	}
	


  open (IN, "<$fname_in") or
    die("ERROR: Can't open '<$fname_in'\n$!\n");

	open (OUT, ">$real_fname_out") or
    die("ERROR: Can't open <$real_fname_out\n$!\n");

  my $seqNo = 0;

	while (<IN>) {
    
          ### skip gene features
          while ( $_ && m"^$GENE_BEGIN" ) {
            $_ = <IN>;
                        
            while ( $_ && m"^$GENE_END" ) {
              $_ = <IN>;
            }
          }

          print OUT;
    
          if (m|^//|) {
            ++$seqNo;
          }
        }

  close (OUT);
  close (IN);


	if ( !(defined($fname_out) && ($fname_out ne $fname_in)) ) {

		#in-place editing, copy the new file over the old one
		rename($real_fname_out, $fname_in);
	}
  return $seqNo;# return number of sequences in file
}

1; 
