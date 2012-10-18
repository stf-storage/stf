use strict;
use Test::More;

use_ok "STF::API::Notification";
use_ok "STF::API::NotificationRule";

subtest "pattern" => sub {
    my $pattern = STF::API::NotificationRule::Matcher->new(
        notifier_name => "Foo",
        operation     => "eq",
        op_field      => "type",
        op_arg        => "storage.downed"
    );

    ok $pattern->match( {
        type => "storage.downed"
    }), "type => 'storage.downed' matches";
    ok ! $pattern->match( {
        type => "storage.up"
    }), "type => 'storage.up' does not match";
};

done_testing;