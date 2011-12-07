use strict;
use Plack::Builder;
use STF::Environment;
use STF::Dispatcher;
use STF::Dispatcher::PSGI;

my $dispatcher = STF::Dispatcher->bootstrap;
STF::Dispatcher::PSGI->new( impl => $dispatcher )->to_app;

