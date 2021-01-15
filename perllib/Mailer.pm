package Mailer;


use strict;
require Utils;
use Exporter ();
use vars qw(@ISA @EXPORT);

@ISA = qw (Exporter);

@EXPORT = qw (open_mailer
              send_mail
              send_mail_file);

=head1 DESCRIPTION

This module provides functions which facilitate the sending of emails.
Messages are sent using /usr/bin/sendmail

=head1 PUBLIC FUNCTIONS

=cut

sub open_mailer{

=over 4

=item C<open_mailer>

=for html
<pre>

USAGE:
       $mailer = open_mailer( {     to => 'whoever@ebi.ac.uk',
                                  from => 'me@ebi.ac.uk',
                               subject => 'Project report' } );
                               
       print( $mailer "Blah blah blah" );

       close( $mailer );

=for html
</pre>

Returns a file handler to which the body of a message can be written.
If the I<from> header is missing it is created by sendmail on the basis
of the OS username.

WARNING: No check is made on the validity of the 'to' address.

=cut


  my( $header ) = @_;

  if ( !defined( $header->{to} ) ) {
    Utils::my_die( "Undefined 'to' header.\n" );
  }

  Utils::my_open( \*SENDMAIL, "|/usr/sbin/sendmail -oi -t" );   # -oi -> . is not end of file
                                                   # -t -> use header for address etc.

  if ( $header->{from} ){
    print( SENDMAIL "From: $header->{from}\n" );
  }
  print( SENDMAIL "To: $header->{to}\n" );
  print( SENDMAIL "Subject: $header->{subject}\n\n" );

  return \*SENDMAIL;
}


sub send_mail {

=item C<send_mail>

=for html
<pre>

USAGE:
       send_mail( {     to => 'whoever@ebi.ac.uk',
                      from => 'me@ebi.ac.uk',
                   subject => 'Project report',
                      body => 'Blah blah blah'    } );

=for html
</pre>

Sends a mail message.

WARNING: No check is made on the validity of the 'to' address.

=cut


  my( $mail ) = @_;

  if ( !exists( $mail->{body} ) ){
    Utils::my_die( "Missing 'body' element.\n" );
  }

  my $body = $mail->{body};
  delete( $mail->{body} );

  my $mailer = open_mailer( $mail );
  print( $mailer $body );
  close( $mailer );
}


sub send_mail_file {

=item C<send_mail_file>

=for html
<pre>

USAGE:
       send_mail_file( {     to => 'whoever@ebi.ac.uk',
                           from => 'me@ebi.ac.uk',
                        subject => 'Project report'    },
                        'file_name'                       );

=for html
</pre>

Sends a mail message, reads the body of the message from a file.

WARNING: No check is made on the validity of the 'to' address.

=cut


  my( $mail, $fname ) = @_;
  if ( !defined( $fname ) ) {
    Utils::my_die( "File name unspecified" );
  }

  my $mailer = open_mailer( $mail );
  Utils::my_open( \*BODY, $fname );

  while ( <BODY> ) {
    print( $mailer $_ );
  }

  close( BODY );
  close( $mailer );
}

1;
=back
