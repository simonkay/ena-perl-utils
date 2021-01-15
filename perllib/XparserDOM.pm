package XparserDOM;

require Exporter;

@ISA = qw (Exporter);

@EXPORT = qw (Xparse);

use strict;
use XML::Parser;

use Data::Dumper;


my ($TAG_STACK, %TREE, $ROOT_TAG, $WS_ONLY);
$WS_ONLY = qr/^\s*$/; # white space only, we can disregard it

sub Xparse {
# Xparse (<$file>, <\%handlers>)
# Parses file <$file> calling the appropriate handlers as specified in
#  the hash <\%handlers>
#
# RETURN: always 1, die if an error is met
#

  my ($file, $handlers) = @_;

  if (1 < keys(%$handlers)) {
     die ("Can't have more than one handler.\n\n");
  }

	($ROOT_TAG) = keys(%$handlers);
	my $tree_handler = $handlers->{$ROOT_TAG};
	
  ($TAG_STACK, %TREE) = (undef,);

  my ($tag_info);

	my $parser = new XML::Parser(ErrorContext => 2);

	$parser->setHandlers(
		Char => \&char_handler,
		Default => \&default_handler,
  	End => sub{end_handler($tree_handler, @_)},
  	Start => \&start_handler
	);

	$parser->parsefile($file);

}

sub start_handler {
	#
	#
	#
	my ($parser, $tag_name, %attributes) = @_;
#	print "<$tag_name";

#	while ( my($k, $v) = each (%attributes) ) {
#		print " $k='$v'";
#	}
#	print " >";

	if ( defined($TAG_STACK) ) {

		my $tag_info = {};
		$tag_info->{name} = $tag_name;
		$tag_info->{attributes} = \%attributes;
		my $parent = $$TAG_STACK[$#$TAG_STACK];

		push(@{$parent->{children}{$tag_name}}, $tag_info);
		push(@$TAG_STACK, $tag_info);
		
	} elsif($tag_name eq $ROOT_TAG) {
	
	  my $tag_info = {};
		$tag_info->{attributes} = \%attributes;
		$tag_info->{name} = $tag_name;
		$TAG_STACK = [$tag_info];
		
	}
}


sub char_handler {
  #
  #
	#
  my ($parser, $content) = @_;
    
#	print "($content)";

	if ( defined($TAG_STACK) && $content !~ /$WS_ONLY/ ) {

    my $parent = $$TAG_STACK[-1];
    $parent->{content} .= $content;
	}
}


sub end_handler {
	#
	#
	#
	my ($tree_handler, $parser, $tag_name) = @_;

#	print "</$tag_name>";

	if ($tag_name eq $ROOT_TAG) {

		&$tree_handler($TAG_STACK->[0]);
		undef($TAG_STACK);
	
	}	elsif ( defined($TAG_STACK) ) {

		pop(@$TAG_STACK);
	}
}


sub default_handler {
	#
	#
	#
}


return 1;
