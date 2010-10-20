use strict;
use warnings;
use ExtUtils::MakeMaker;
use Config;

my ($thrift) = map { /^THRIFT=(.+)/ } @ARGV;
@ARGV = grep { !/^THRIFT=/ } @ARGV;

unless ($thrift) {
    ($thrift) = grep { -e "$_/include/thrift/Thrift.h" }
                  qw( /usr/local /usr )
      or die "Can't find Thrift header files -- is Thrift dev installed?\n";
}

my ($thrift_a, $thrift_lib) = ("$thrift/lib/libthrift.a", '');
unless (-e $thrift_a) {
    warn "WARNING: Could not find libthrift.a.  Check Cassandra.so for undefined externals\n";
    ($thrift_a, $thrift_lib) = ('', "-L$thrift/lib -lthrift");
}

my $cppflag;
if ($Config{ccname} eq 'gcc') {
    $cppflag = '-W -Wall -x c++ -o $*$(OBJ_EXT)';  # for release, I think I'll remove -W
}
else {
    die "Edit me to know how to compile C++ to a target dir on your system, and mail a patch";
}

WriteMakefile(
    NAME                => 'Cassandra',
    AUTHOR              => q{Chip Salzenberg <chip@pobox.com>},
    VERSION_FROM        => 'Cassandra.pm',
    ABSTRACT_FROM       => 'Cassandra.pm',
    ($ExtUtils::MakeMaker::VERSION >= 6.3002
      ? ('LICENSE'=> 'perl')
      : ()),
    #DEFINE              => '-DPERL_EXT',
    XSOPT               => '-C++',
    macro               => { VERSION_MACRO => 'MODULE_VERSION' },
    INC                 => qq[-Igen-cpp -I$thrift/include/thrift],
    CCFLAGS             => qq[$cppflag],
    OBJECT              => qq[XCass.o gen-cpp/Cassandra.o gen-cpp/cassandra_constants.o gen-cpp/cassandra_types.o $thrift_a],
    LIBS                => qq[$thrift_lib],
    PL_FILES            => {},
    PREREQ_PM => {
        'Test::More' => 0,
    },
    dist                => { COMPRESS => 'gzip -9f', SUFFIX => 'gz', },
    clean               => { FILES => 'Cassandra-*' },
);
