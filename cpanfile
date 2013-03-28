#!perl
requires 'Cache::Memcached::Fast' => 0;
requires 'Data::Page' => 0;
requires 'Data::FormValidator' => 0;
requires 'Data::Localize' => 0;
requires 'Digest::MurmurHash' => 0;
requires 'DBI' => 0;
requires 'DBD::mysql' => 0;
requires 'Email::MIME' => 0;
requires 'Email::Send' => 0;
requires 'File::RotateLogs' => '0.02';
requires 'Furl' => '0.38';
requires 'HTML::FillInForm::Lite' => 0;
requires 'HTTP::Parser::XS' => 0;
requires 'IPC::SysV' => 0;
requires 'Log::Minimal' => '0.14';
requires 'Math::Round' => 0;
requires 'Mojolicious' => '3.85';
requires 'Mouse' => 0;
requires 'Module::Build' => '0.4003';
requires 'Net::SNMP' => 0;
requires 'Plack' => '1.0013';
requires 'Plack::Middleware::AxsLog' => '0.10';
requires 'Plack::Middleware::ReverseProxy' => 0;
requires 'Plack::Middleware::Session' => 0;
requires 'Plack::Middleware::Static' => 0;
requires 'Plack::Request' => 0;
requires 'Plack::Session' => 0;
requires 'Parallel::ForkManager' => '0.7.9';
requires 'Parallel::Scoreboard' => '0.03';
requires 'Router::Simple' => 0;
requires 'SQL::Maker' => 0;
requires 'Scope::Guard' => 0;
requires 'Starlet' => 0;
requires 'Server::Starter';
requires 'String::Urandom' => 0;
requires 'Task::Weaken' => 0;
requires 'Text::Xslate' => '1.6001';
requires 'YAML' => 0;
requires 'STF::Dispatcher::PSGI' => '1.09';

# Add these requirement(s) if the environment asks for it
my $queue_type = $ENV{STF_QUEUE_TYPE} || 'Q4M';
if ($queue_type eq 'Schwartz') {
    requires 'TheSchwartz' => 0;
} elsif ($queue_type eq 'Redis') {
    requires 'Redis'       => 0;
} elsif ($queue_type eq 'Resque') {
    requires 'Resque'      => 0;
}

# You need these if you don't have 64 bit ints
# HIGHLY RECOMMENDED THAT YOU USE PERL WITH 64BIT INTS
if (! $Config::Config{use64bitint}) {
    requires "Bit::Vector" => 0;
    requires "Math::BigInt" => 0;
}

on build => sub {
    requires 'App::Prove' => 0;
    requires 'Proc::Guard' => 0;
    requires 'Test::TCP' => 0;
    requires 'Test::mysqld' => 0;
    requires 'Test::MockTime' => 0;
    requires 'Plack::Middleware::Reproxy' => '0.00002';
};

