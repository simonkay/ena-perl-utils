package ASCII_Table;
# Exports a few functions to handle 2D arrays
# The first row contains column headers
#


use strict;
use Exporter ();
use vars qw(@ISA @EXPORT);

@ISA = qw (Exporter);

@EXPORT = qw (ascii_table
              filter
              remove_column);


sub ascii_table {
   # Converts a 2D array into a string containing the array content in
   #  table format (space padded).
   # The first row of the array contains the column titles
   #
   
   my ($table_arr) = @_;
   
   my $padding = ' ';
   
   my $ascii_table = '';
   
   format_table ($table_arr);
   
   my ($row, $cell);
   foreach $row (@$table_arr){
      foreach $cell (@$row){
         if ($cell) {
            $ascii_table .= "$cell$padding";
         }
      }
      $ascii_table .= "\n";
   }
   
   return ($ascii_table);
}


sub format_table {
   # Makes all the cells in a column of the same length by inserting spaces.
   # A separator line is introduced between the first and second rows.
   # The last column is not padded
   #
   
   my ($table_arr) = @_;
   
   my $max_len_arr = get_maxlen_arr ($table_arr);
   
   my $row;
   foreach $row (@$table_arr){
      for (my $i = 0; $i < $#$row; ++$i) {# we don't need to format the last cell in a row
         chomp ($$row[$i]);
         if ( $$max_len_arr[$i] > length ($$row[$i]) ) {
            $$row[$i] .= ' ' x ( $$max_len_arr[$i] - length ($$row[$i]) );
         }
      }
   }
   
   my @separators;
   for (my $i = 0; $i <= $#{$$table_arr[0]}; ++$i) {
      push (@separators, '-' x $$max_len_arr[$i]);
   }
   
   unshift (@$table_arr, \@separators);
   ($$table_arr[0], $$table_arr[1]) = ($$table_arr[1], $$table_arr[0]);
}



sub get_maxlen_arr {
   # RETURN an array containing the maximum cell length for each column in
   #  <$table_arr>
   #
   
   my ($table_arr) = @_;
   
   my @max_lengths;
   my $row;
   
   foreach $row (@$table_arr){
      
      for (my $i = 0; $i <= $#$row; ++$i) {
         
         unless ( defined($$row[$i]) ){
            $$row[$i] = ' ';
         }
         
         if (!defined($max_lengths[$i]) || ($max_lengths[$i] < length ($$row[$i])) ) {
            $max_lengths[$i] = length ($$row[$i]);
         }
         
      }
   }
   
   return (\@max_lengths);
}



sub filter {
   # Removes rows from <$table_arr> where the value of the
   #   <$col>th column is not equal to <$value>
   # (first column is 1)
   # Does nothing if <$value> evaluates to flase
   #
   
   my ($table_arr, $col, $value) = @_;
   
   unless ($value){
      return;
   }
   
   --$col;
   
   my @filtered_table;
   my $row;
   foreach $row (@$table_arr){
      
      if ( $$row[$col] && ($$row[$col] eq $value) ) {
         
         push (@filtered_table, $row);
      }
   }
   
   @$table_arr = @filtered_table;
}



sub remove_column {
   # remove the <$col>th column from <$table_arr>
   # (first column is 1)
   #
   
   my ($table_arr, $col) = @_;
   if ($col < 1) {
      die ("Wrong arguments for remove_column (first column is 1)");
   }
   --$col;
   
   my $row;
   
   foreach $row (@$table_arr){
      
      splice (@$row, $col, 1);
   }
}
