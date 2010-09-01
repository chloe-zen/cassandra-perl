#!perl -T

use Test::More tests => 1;

BEGIN {
    use_ok( 'Cassandra' ) || print "Bail out!\n";
}

diag( "Testing Cassandra $Cassandra::VERSION, Perl $], $^X" );
