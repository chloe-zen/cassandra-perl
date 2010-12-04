package Cassandra;
use warnings;
use strict;
require Carp;  # called from XS

=head1 NAME

Cassandra - Fast and complete interface to Cassandara database

=head1 VERSION

Version 0.01

=cut

our $VERSION = '0.01';
our $XS_VERSION = $VERSION;

if (eval { require XSLoader }) {
    XSLoader::load('Cassandra', $XS_VERSION);
}
else {
    require DynaLoader;
    local @Cassandra::ISA = qw(DynaLoader);
    Cassandra->bootstrap($XS_VERSION);
}

=head1 SYNOPSIS

    use Cassandra;

    my $foo = Cassandra->new(server => $server, keyspace => $ks);
    ...

=head1 EXPORT

None - this is all OO baby.

=head1 METHODS

=cut

use Exporter 'import';
our (@EXPORT, @EXPORT_OK, @EXPORT_TAGS);
BEGIN {
    my %c = (
             CONSISTENCY_ONE => 1,
             CONSISTENCY_QUORUM => 2,
             CONSISTENCY_LOCAL_QUORUM => 3,
             CONSISTENCY_EACH_QUORUM => 4,
             CONSISTENCY_ALL => 5,
             CONSISTENCY_ANY => 6,
            );
    push @EXPORT_OK, keys %c;
    $EXPORT_TAGS{CONST} = \%c;

    require constant;
    constant->import(\%c);
}

=head2 function1

=cut

sub function1 {
}

=head2 function2

=cut

sub function2 {
}

1;
__END__

=head1 AUTHOR

Chip Salzenberg, C<< <chip@pobox.com> >>

=head1 BUGS

Please report any bugs or feature requests to C<bug-cassandra at rt.cpan.org>, or through
the web interface at L<http://rt.cpan.org/NoAuth/ReportBug.html?Queue=Cassandra>.
I will be notified, and then you'll automatically be notified of progress on your bug as I make changes.

=head1 SUPPORT

You can find documentation for this module with the perldoc command.

    perldoc Cassandra

You can also look for information at:

=over 4

=item * RT: CPAN's request tracker

L<http://rt.cpan.org/NoAuth/Bugs.html?Dist=Cassandra>

=item * AnnoCPAN: Annotated CPAN documentation

L<http://annocpan.org/dist/Cassandra>

=item * CPAN Ratings

L<http://cpanratings.perl.org/d/Cassandra>

=item * Search CPAN

L<http://search.cpan.org/dist/Cassandra/>

=back


=head1 ACKNOWLEDGEMENTS


=head1 LICENSE AND COPYRIGHT

Copyright 2010 Topsy Labs.

This program is free software; you can redistribute it and/or modify it
under the terms of either: the GNU General Public License as published
by the Free Software Foundation; or the Artistic License.

See http://dev.perl.org/licenses/ for more information.


=cut
