use strict;
use Test::More;
BEGIN {
    use_ok "STF::Context";
}

{
    my $cxt = STF::Context->bootstrap;
    my $container = $cxt->container;

    my $notification = $container->get('API::Notification');

    ok $notification;

    $notification->create({
        ntype => "hello.world",
        message => "Hello, World!"
    });

    # XXX FIXME
    ok 1;
};

done_testing;
