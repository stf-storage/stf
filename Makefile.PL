use strict;
use ExtUtils::MakeMaker;
use Config;

# The additional \n helps to fool Module::Install
sub MY::test_via_harness { "\tprove --lib --rc=t/proverc t\n" }

my $mysql = `which mysql`;
chomp $mysql;
$mysql ||= '(null)';
my $mysqladmin = `which mysqladmin`;
chomp $mysqladmin;
$mysqladmin ||= '(null)';

print <<EOM;
mysql and mysqladmin executables:
 + mysql: $mysql
 + mysqladmin: $mysqladmin
EOM

if ( $mysql eq '(null)' || $mysqladmin eq '(null)' ) {
    print STDERR <<EOM;
Hmm, I couldn't find mysql and/or mysqladmin executables.
STF tests requires 'mysql' and 'mysqladmin' to be in available in our PATH.
Please make sure to put them in to build/test this application.

    export PATH=/path/to/mysql/bin:\$PATH
    perl Makefile.PL
    make test

EOM
}

if (! $Config{use64bitint}) {
    print STDERR <<EOM;
STF requires 64 bit ints, and your perl doesn't seem to have use64bitint
defined :/

EOM
    exit 1;
}

WriteMakefile(
    NAME => "STF",
    VERSION_FROM => 'lib/STF.pm',
    LICENSE => "artistic_2",
    AUTHOR    => 'Daisuke Maki C<< <daisuke@endeworks.jp> >>',
    PREREQ_PM => {
        # Only Migrator requires these
        'Coro' => 0,
        'FurlX::Coro' => 0,
        'DBIx::Connector' => 0,

        'Cache::Memcached::Fast' => 0,
        'Class::Accessor::Lite' => 0,
        'Class::Load' => 0,
        'Data::Page' => 0,
        'Data::FormValidator' => 0,
        'Digest::MurmurHash' => 0,
        'DBI' => 0,
        'DBD::mysql' => 0,
        'Furl' => '0.38',
        'Guard' => 0,
        'HTML::FillInForm' => 0,
        'HTTP::Parser::XS' => 0,
        'IPC::SysV' => 0,
        'Plack' => '0.9981',
        'Plack::Middleware::ReverseProxy' => 0,
        'Plack::Middleware::Static' => 0,
        'Parallel::Prefork' => 0,
        'Parallel::ForkManager' => 0,
        'Parallel::Scoreboard' => 0,
        'Router::Simple' => 0,
        'SQL::Maker' => 0,
        'Starlet' => 0,
        'String::Urandom' => 0,
        'Text::Xslate' => 0,
        'YAML' => 0,
        'STF::Dispatcher::PSGI' => '1.01',
    },
    BUILD_REQUIRES => {
        # For tests
        'App::Prove' => 0,
        'Proc::Guard' => 0,
        'Test::TCP' => 0,
        'Test::mysqld' => 0,
        'Test::MockTime' => 0,
        'Plack::Middleware::Reproxy' => '0.00002',
    },
    clean => {
        FILES => 't/*.log t/store*',
    }
);

