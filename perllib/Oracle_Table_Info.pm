package Oracle_Table_Info;
#
# Some functions to retrieve information about the logical structure of Oracle
#  tables.
# 
# The database connection (from DBI) must be supplied by the calling program
#
# All functions with the exception of <get_table_comment> return a ref to a 2D
#  array with column names in the first row.
#

use ASCII_Table;


require Exporter;
use vars qw(@ISA @EXPORT);

@ISA = qw (Exporter);

@EXPORT = qw (
  table_exists
  get_table_comment
  get_table_columns
  get_table_primary_key
  get_table_foreign_key_IN
  get_table_foreign_key_OUT
  get_table_check_constraints
  get_table_privileges
  get_table_indexes
  set_sql_indent
  get_table_comment_sql
  get_table_columns_sql
  get_table_primary_key_sql
  get_table_foreign_keys_sql
  get_table_check_constraints_sql
  get_table_access_privileges_sql
  get_table_indexes_sql);


use strict;

my $INDENT = ' ' x 3;


sub table_exists {
   #
   #
   #

   my ($dbh, $owner, $table_name) = @_;

   my $sql = <<sqlEND;
SELECT
       table_name
       
  FROM
       all_tables
       
  WHERE
       table_name = UPPER('$table_name')
sqlEND

   if ($owner) {
      $sql .= "AND owner = UPPER('$owner')";
   }

   my $result = $dbh->selectall_arrayref ($sql);

   return (scalar(@$result));
}


sub get_table_comment {
  #
  #
  #

  my ($dbh, $owner, $table_name) = @_;

  my $sql = <<sqlEND;
SELECT
        comments,
        owner
  FROM
        all_tab_comments
  WHERE
        table_name = '$table_name'
sqlEND

  my $comments_arr = $dbh->selectall_arrayref ($sql);

  if ( !defined($comments_arr) ) {
    $dbh->disconnect ();
    die ("ERROR: could not fetch comment for table $table_name!\n");
  }

  filter ($comments_arr, 2, $owner);
  remove_column ($comments_arr, 2);

  if ( $#$comments_arr > 1 ) {
    die ("ERROR: more than one comment for table $table_name!\n");
  }
  unless (defined ($$comments_arr[0][0])){
     $$comments_arr[0][0] = '';
  }
  return ($$comments_arr[0][0]);
}


sub get_table_columns {
  # Column name | Data type | Nullable | Default [| Comments]
  # Comments are included only if <$comments> evaluates to true.
  #

  my ($dbh, $owner, $table_name, $comments) = @_;

  my ($sql, $headers, $owner_col);

  unless ($comments) {
    $sql = <<sqlEND;
SELECT
       t.column_name,
       t.data_type || decode (t.data_type,
                              'CHAR',     '(' || t.data_length || ')',
                              'VARCHAR',  '(' || t.data_length || ')',
                              'VARCHAR2', '(' || t.data_length || ')',
                              'NUMBER',
                                        decode (t.data_precision,
                                                NULL, NULL,
                                                '(' || t.data_precision || decode (t.data_scale,
                                                                                   0, ')',
                                                                                   ',' || t.data_scale || ')'
                                                                                  )
                                                )
                              ),
        t.nullable,
        t.data_default,
        c.comments,
        t.owner
  FROM
        all_tab_columns t,
        all_col_comments c
  WHERE
        t.owner = c.owner
    AND t.column_name = c.column_name
    AND t.table_name = c.table_name
    AND t.owner = c.owner
    AND t.column_name = c.column_name

    AND t.table_name = \'$table_name\'
  ORDER BY
        t.column_id ASC
sqlEND

    $headers = ['Name', 'Data Type', 'Nullable', 'Default', 'Comments'];
    $owner_col = 6;

  }else{
    $sql = <<sqlEND;
SELECT
       column_name,
       data_type || decode (data_type,
                            'CHAR',     '(' || data_length || ')',
                            'VARCHAR',  '(' || data_length || ')',
                            'VARCHAR2', '(' || data_length || ')',
                            'NUMBER',
                                     decode (data_precision,
                                             NULL, NULL,
                                             '(' || data_precision || decode (data_scale,
                                                                              0, ')',
                                                                              ',' || data_scale || ')'
                                                                             )
                                             )
                            ),
        nullable,
        data_default,
        owner
  FROM
        all_tab_columns
  WHERE
        table_name = \'$table_name\'

  ORDER BY
        column_name
sqlEND

    $owner_col = 5;
    $headers = ['Name', 'Data Type', 'Nullable', 'Default'];
  }

  my $columns_arr = $dbh->selectall_arrayref ($sql);

  filter ($columns_arr, $owner_col, $owner);
  remove_column ($columns_arr, $owner_col);

  unshift (@$columns_arr, $headers);

  return ($columns_arr);
}


sub get_table_primary_key {
  #
  #
  #

  my ($dbh, $owner, $table_name, $type) = @_;

  my $sql = <<sqlEND;
SELECT
        ac.constraint_name,
        accol.column_name,
        ac.deferred,
        ac.status,
        ac.owner

  FROM
        all_constraints ac,
        all_cons_columns accol

  WHERE
        ac.constraint_name = accol.constraint_name
    AND ac.constraint_type = 'P'
    AND ac.table_name = '$table_name'

  ORDER BY
        accol.position
sqlEND

  my $primary_key_arr = $dbh->selectall_arrayref($sql);
  filter ($primary_key_arr, 5, $owner);
  remove_column ($primary_key_arr, 5);

  unshift (@$primary_key_arr, ['Name', 'Column', 'Deferred', 'Status']);

  return ($primary_key_arr);
}


sub get_table_foreign_key_OUT {
  #
  #
  #

  my ($dbh, $owner, $table_name) = @_;

  my $sql = <<sqlEND;
SELECT
        origin.constraint_name,
        origin.status,
        origin.deferred,
        origin.delete_rule,
        origincol.column_name,
        destcol.table_name || '.' || destcol.column_name,
        origin.owner

  FROM
        all_constraints origin,
        all_cons_columns destcol,
        all_cons_columns origincol

  WHERE
        origin.constraint_name = origincol.constraint_name
    AND origin.table_name = origincol.table_name
    AND origin.owner = origincol.owner
    AND origin.r_constraint_name = destcol.constraint_name

    AND origin.constraint_type = 'R'
    AND origincol.table_name = '$table_name'

ORDER BY
        origincol.position
sqlEND

  my $foreign_key_arr_OUT = $dbh->selectall_arrayref ($sql);
  filter ($foreign_key_arr_OUT, 7, $owner);
  remove_column ($foreign_key_arr_OUT, 7);

  unshift (@$foreign_key_arr_OUT, ['Name', 'Status', 'Deferred', 'Delete', 'Column', 'To']);

  return $foreign_key_arr_OUT;
}


sub get_table_foreign_key_IN {
  #
  #
  #

  my ($dbh, $owner, $table_name) = @_;

  my $sql = <<sqlEND;
SELECT
        origin.constraint_name,
        origin.status,
        origin.deferred,
        origin.delete_rule,
        destcol.column_name,
        origincol.table_name || '.' || origincol.column_name,
        origin.owner

  FROM
        all_constraints origin,
        all_cons_columns destcol,
        all_cons_columns origincol

  WHERE
        origincol.constraint_name = origin.constraint_name
    AND origincol.table_name = origin.table_name
    AND origincol.owner = origin.owner
    AND destcol.constraint_name = origin.r_constraint_name

    AND origin.constraint_type = 'R'
    AND destcol.table_name = '$table_name'

ORDER BY
        origincol.position
sqlEND

  my $foreign_key_arr_IN = $dbh->selectall_arrayref ($sql);

  filter ($foreign_key_arr_IN, 7, $owner);
  remove_column ($foreign_key_arr_IN, 7);

  unshift (@$foreign_key_arr_IN, ['Name', 'Status', 'Deferred', 'Delete', 'Column', 'From']);

  return $foreign_key_arr_IN;
}


sub get_table_check_constraints {
  #
  #
  #

  my ($dbh, $owner, $table_name) = @_;

  my $sql = <<sqlEND;
SELECT
       constraint_name,
       status,
       deferred,
       search_condition,
       owner

  FROM
       all_constraints

  WHERE
       constraint_type IN ('C')
   AND table_name = \'$table_name\'

  ORDER BY
    constraint_name
sqlEND

  my $check_constraints_arr = $dbh->selectall_arrayref ($sql);

  filter ($check_constraints_arr, 5, $owner);
  remove_column ($check_constraints_arr, 5);

  unshift (@$check_constraints_arr, ['Name', 'Status', 'Deferred', 'Check']);

  return ($check_constraints_arr);
}

sub get_table_privileges {
  #
  #
  #

  my ($dbh, $table_name) = @_;

  my $sql = <<sqlEND;
SELECT
        privilege,
        grantee,
        grantable

  FROM
        all_tab_privs

  WHERE
        table_name = '$table_name'

  ORDER BY
        grantee
sqlEND

  my $privileges_arr = $dbh->selectall_arrayref ($sql);

  unshift (@$privileges_arr, ['Action', 'Grantee', 'Grantable']);

  return ($privileges_arr);
}



sub get_table_indexes {
  #
  #
  #
  my ($dbh, $owner, $table_name) = @_;

  my $sql = <<sqlEND;
SELECT 
       i.index_name,
       decode (i.uniqueness, 'UNIQUE', 'Yes', 'No') uniqueness,
       ic.column_name,
       i.owner
  
  FROM 
       all_indexes i,
       all_ind_columns ic
  
  WHERE
        i.table_name = ic.table_name
    AND i.index_name = ic.index_name
    AND i.table_name = '$table_name'

  ORDER BY 
        i.index_name, ic.column_position
sqlEND

  my $indexes_arr_temp = $dbh->selectall_arrayref ($sql);

  filter ($indexes_arr_temp, 4, $owner);
  remove_column ($indexes_arr_temp, 4);

  
  # now put all the column names for each index in the same line
  my @privileges_arr_final;

  my $index_name = '';
  my $index_name_col = 0;
  my $column_name_col = 2;

  foreach (@$indexes_arr_temp) {
    
    if ($$_[$index_name_col] ne $index_name) {
      push (@privileges_arr_final, $_);
      $index_name = $$_[$index_name_col];

    }else{
      $privileges_arr_final[$#privileges_arr_final][$column_name_col] .= ", $$_[$column_name_col]";
    }
  }

  unshift (@privileges_arr_final, ['Name', 'Unique?', 'Columns']);

  return \@privileges_arr_final;
}


sub set_sql_indent {
   #
   #
   #

   my ($indent) = @_;

   if ($indent =~ /^\d+$/) {
      $INDENT = ' ' x $indent;
   
   }else{
      $INDENT = $indent;
   }

}

sub get_table_comment_sql {
   #
   #
   #

   my ($dbh, $owner, $table_name) = @_;

   my $table_comment = get_table_comment($dbh, $owner, $table_name);

   my $statement;
   if ($table_comment) {
     $statement = <<sqlEND;
COMMENT ON TABLE $table_name IS
${INDENT}'$table_comment';

sqlEND

   }else{
     $statement = '';
   }
   
   return $statement;
}


sub get_table_columns_sql {
   #
   #
   #

   my ($dbh, $owner, $table_name, $comments) = @_;

   my $table_descr_arr_ref = get_table_columns($dbh, $owner, $table_name, $comments);

   shift (@$table_descr_arr_ref);# lose the title row

   # columns :'Name', 'Data Type', 'Nullable', 'Default' [,'Comments']

   my $statement = "CREATE TABLE $table_name(\n";
   my $comments_sql = '';

   foreach (@$table_descr_arr_ref) {
      $statement .= "${INDENT}${$_}[0] ${$_}[1]";

      unless (${$_}[2]) {# not nullable
         $statement .= " NOT NULL";
      }

      if (${$_}[3]) {# default
         $statement .= " DEFAULT ${$_}[3]";
      }

      $statement .= ",\n";

      if (${$_}[4]) {
         $comments_sql .= "COMMENT ON COLUMN ${table_name}.${$_}[0] IS\n${INDENT}'${$_}[4]';\n\n";
      }
   }

   substr ($statement, -2, 2) = "\n${INDENT});\n\n";

   if ($comments_sql && !$comments) {
      $statement .= $comments_sql;
   }
   return ($statement);
}


sub get_table_primary_key_sql {
   #
   #
   #

   my ($dbh, $owner, $table_name) = @_;

   my $sql = <<sqlEND;
SELECT
       c.constraint_name,
       t.tablespace_name,
       t.initial_extent,
       t.next_extent,
       DECODE( deferrable, 'DEFERRABLE', 'DEFERRABLE', '' )||
       DECODE( deferred, 'DEFERRED', ' INITIALLY DEFERRED', '' ) defer
  FROM
       all_constraints c,
       all_tables t
  WHERE
       c.constraint_type = 'P'
   AND t.table_name = c.table_name
   AND t.table_name = '$table_name'
sqlEND

   if ($owner) {
      $sql .= "\nAND t.owner = '$owner'";
   }

   my $primary_key_array_ref = $dbh->selectall_arrayref ($sql);

   my @commands;

   foreach my $row ( @$primary_key_array_ref ) {

      my ( $constraint_name, $tablespace_name,
           $initial_extent, $next_extent, $defer ) = @{$row};

      my $sql = <<sqlEND;
SELECT
       position,
       column_name

  FROM
       all_cons_columns

  WHERE
       table_name      = '$table_name'
   AND constraint_name = '$constraint_name'

  ORDER BY
       position
sqlEND

      my $columns_array_ref = $dbh->selectall_arrayref ($sql);

      my @columns = map (${$_}[1], @$columns_array_ref);

      my $statement = <<sqlEND;
ALTER TABLE $table_name
${INDENT}ADD CONSTRAINT $constraint_name
${INDENT}PRIMARY KEY ( @columns )
sqlEND

      if ( defined $defer ) {
        $statement .= "\n".$defer;
      }

      $statement .= "${INDENT}USING INDEX TABLESPACE $tablespace_name\n";
      $statement .= "${INDENT}STORAGE ( INITIAL $initial_extent NEXT $next_extent );\n\n";
   
      push(@commands, $statement);
   }

   return (@commands);
}



sub get_table_foreign_keys_sql {
   #
   #
   #

   my ($dbh, $owner, $table_name) = @_;

   my $sql = <<sqlEND;
SELECT
       c.constraint_name,
       c.r_constraint_name,
       DECODE( c.delete_rule, 'CASCADE', 'ON DELETE CASCADE', '' ),
       DECODE( c.deferrable, 'DEFERRABLE', 'DEFERRABLE', '' )||
       DECODE( c.deferred, 'DEFERRED', ' INITIALLY DEFERRED', '' ) defer

  FROM
       all_constraints c

  WHERE
       c.constraint_type = 'R'
   AND c.table_name = '$table_name'
sqlEND

   if ($owner) {
      $sql .= "AND owner = '$owner'\n"
   }

   my $constraints_array_ref = $dbh->selectall_arrayref ($sql);

   my @commands;

   foreach my $row ( @$constraints_array_ref ) {
      my ( $constraint_name, $r_constraint_name, $delete, $defer ) = @{$row};

      my $sql = <<sqlEND;
SELECT
       position,
       column_name

  FROM
       all_cons_columns

  WHERE
       table_name      = '$table_name'
   AND constraint_name = '$constraint_name'
sqlEND

      if ($owner) {
         $sql .= "AND owner = '$owner'\n"
      }

   $sql .= <<sqlEND;
  ORDER BY
       position
sqlEND

      my $constraint_ref = $dbh->selectall_arrayref ($sql);

      my $columns_str = join (', ', map (${$_}[1], @$constraint_ref) );

      my $statement = <<sqlEND;
ALTER TABLE $table_name
${INDENT}ADD CONSTRAINT $constraint_name
${INDENT}FOREIGN KEY ( $columns_str )
sqlEND

      $sql = <<sqlEND;
SELECT
       table_name

  FROM
       all_constraints

  WHERE
       constraint_name = '$r_constraint_name'
sqlEND

      my $r_table_name_arr_ref = $dbh->selectall_arrayref ($sql);
      my $r_table_name = ${$r_table_name_arr_ref}[0][0];

      $sql = <<sqlEND;
SELECT
       DECODE( position, 1, '', ', ') || column_name

  FROM
       all_cons_columns

  WHERE
       table_name      = '$r_table_name'
   AND constraint_name = '$r_constraint_name'

  ORDER BY
       position
sqlEND

      my $r_columns_arr_ref = $dbh->selectall_arrayref ($sql);

      my $r_columns_str = join (', ', map (${$_}[0], @$r_columns_arr_ref) );

      $statement .= "${INDENT}REFERENCES $r_table_name ( $r_columns_str )";


      if ( defined $delete ) {
        $statement .= "\n${INDENT}".$delete;
      }

      if ( defined $defer ) {
        $statement .= "\n${INDENT}".$defer;
      }

      $statement .= ";\n\n";

      push(@commands, $statement);
   }

   return (@commands);
}



sub get_table_check_constraints_sql {
   #
   #
   #

   my ($dbh, $owner, $table_name) = @_;

   my $sql = <<sqlEND;
SELECT
       cc.column_name,
       c.constraint_name,
       c.search_condition

  FROM
       all_cons_columns cc,
       all_constraints c

  WHERE
       c.constraint_type  = 'C'
   AND cc.table_name      = c.table_name
   AND cc.constraint_name = c.constraint_name
   AND c.table_name       = '$table_name'
sqlEND

   if ($owner) {
      $sql .= "AND cc.owner = '$owner'\n"
   }

   $sql .= <<sqlEND;
  ORDER BY
       c.constraint_name,
       cc.column_name
sqlEND

   my $constraints_arr_ref = $dbh->selectall_arrayref ($sql);

   my $previous_constraint_name = "";

   my @commands;

   foreach my $row ( @$constraints_arr_ref ) {

      my ( $column_name, $constraint_name, $search_condition ) = @{$row};

      # skip NOT NULL constraints (they are in the create table statements)
      # and "duplicate" rows (if a check constraint concerns several columns,
      # the query above will return one row for each column).
      # I could not find a more elegant way to do this because
      # all_constraints.search_condition is a LONG and hence neither LIKE
      # nor DISTINCT can be used...
      next if ( $search_condition =~ /IS NOT NULL$/ );
      next if ( $constraint_name eq $previous_constraint_name );
      $previous_constraint_name = $constraint_name;

      my $statement = <<sqlEND;
ALTER TABLE $table_name
${INDENT}ADD CONSTRAINT $constraint_name
${INDENT}CHECK ( $search_condition );

sqlEND

      push (@commands, $statement);
   }

   return (@commands);
}


sub get_table_access_privileges_sql {
   #
   #
   #

   my ($dbh, $owner, $table_name) = @_;

   my $sql = <<sqlEND;
SELECT
       'GRANT '||privilege||
       ' ON ' || table_name ||
       ' TO ' || grantee ||
       DECODE( grantable, 'YES', ' WITH GRANT OPTION;', ';' )

  FROM
       all_tab_privs

  WHERE
       table_name = '$table_name'

  ORDER BY
       grantee
sqlEND

   my $grants_arr_ref = $dbh->selectall_arrayref ($sql);

   my @commands = map(${$_}[0] .= "\n\n", @$grants_arr_ref);

   return(@commands);
}


sub get_table_indexes_sql {
   #
   #
   #

   my ($dbh, $owner, $table_name) = @_;

   my $sql = <<sqlEND;
SELECT
       i.index_name,
       DECODE( i.uniqueness, 'UNIQUE', 'UNIQUE INDEX','INDEX' ),
       i.tablespace_name,
       i.initial_extent,
       i.next_extent

  FROM
       all_indexes i

  WHERE
       i.index_type  = 'NORMAL'
   AND i.table_name = '$table_name'
sqlEND


   if ($owner) {
      $sql .= "AND i.table_owner = '$owner' ";
   }

   $sql .= <<sqlEND;
   AND NOT EXISTS ( SELECT
                           * -- pk / uq indexes are defined via constraints

                      FROM
                           all_cons_columns c

                      WHERE
                           c.table_name      = i.table_name
                       AND c.constraint_name = i.index_name )

  ORDER BY
       uniqueness,
       index_name
sqlEND

   my $indexes_arr_ref = $dbh->selectall_arrayref ($sql);

   my @commands;

   foreach my $row ( @$indexes_arr_ref ) {

      my ( $index_name, $index_type, $tablespace_name,
           $initial_extent, $next_extent ) = @{$row};

      $sql = <<sqlEND;
SELECT
       column_position,
       column_name

  FROM
       all_ind_columns

  WHERE
       table_name = '$table_name'
   AND index_name = '$index_name'

  ORDER BY
       column_position
sqlEND

      my ( $index_columns_arr_ref ) = $dbh->selectall_arrayref ($sql);

      my $columns_str = join (', ', map (${$_}[1], @$index_columns_arr_ref) );

      my $statement = <<sqlEND;
CREATE ${index_type} ${index_name} ON $table_name
${INDENT}( $columns_str )
${INDENT}TABLESPACE $tablespace_name
${INDENT}STORAGE ( INITIAL $initial_extent NEXT $next_extent );

sqlEND


      push(@commands, $statement);
   }

   return(@commands);
}
