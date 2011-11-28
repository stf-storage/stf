use strict;
use lib "lib";
use Plack::Builder;
use STF::Dispatcher;
use STF::Dispatcher::PSGI;

$ENV{STF_CONFIG} = "t/config.pl";
$ENV{STF_CONTAINER} = "etc/container.pl";
my $stf = STF::Dispatcher::PSGI->new( impl => STF::Dispatcher->bootstrap );

builder {
    enable sub {
        my $app = shift;
        sub { $_[0]->{PATH_INFO} =~ s!/+!/!g; $app->($_[0]) }
    };
    enable 'Reproxy::Furl';
    enable 'ConditionalGET';
    $stf->to_app;
};