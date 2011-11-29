use inc::Module::Install;

name 'STF';
all_from 'lib/STF.pm';

# Only Migrator requires these
requires 'Coro';
requires 'FurlX::Foro';

requires 'Cache::Memcached::Fast';
requires 'Class::Accessor::Lite';
requires 'Class::Load';
requires 'Data::Page';
requires 'Data::FormValidator';
requires 'Digest::MurmurHash';
requires 'DBI';
requires 'DBD::mysql';
requires 'Furl' => '0.38';
requires 'Guard';
requires 'HTML::FillInForm';
requires 'HTTP::Parser::XS';
requires 'IPC::SysV';
requires 'Plack' => '0.9981';
requires 'Plack::Middleware::ReverseProxy';
requires 'Plack::Middleware::Static';
requires 'Parallel::Prefork';
requires 'Parallel::ForkManager';
requires 'Parallel::Scoreboard';
requires 'Router::Simple';
requires 'SQL::Maker';
requires 'Starlet';
requires 'String::Urandom';
requires 'Text::Xslate';
requires 'YAML';
requires 'STF::Dispatcher::PSGI';

test_requires 'App::Prove';
test_requires 'Proc::Guard';
test_requires 'Test::TCP';
test_requires 'Test::mysqld';
test_requires 'Test::MockTime';
test_requires 'Plack::Middleware::Reproxy' => '0.00002';

clean_files 't/*.log', 't/store*';

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
    exit 1;
}

WriteAll;

