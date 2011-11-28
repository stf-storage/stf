use strict;
use Test::More;
use Time::HiRes();

use_ok "STF::Container";

subtest 'basic' => sub {
    my $c = STF::Container->new;

    $c->register( foo => 1 );
    $c->register( bar => sub { Time::HiRes::time() } );
    $c->register( baz => sub { Time::HiRes::time() }, { scoped => 1 } );

    my ($t_bar, $t_baz);
    {
        my $scope = $c->new_scope;

        is $c->get('foo'), 1;
        ok $t_bar = $c->get('bar');
        ok $t_baz = $c->get('baz');
    }

    {
        my $scope = $c->new_scope;
        is $c->get('foo'), 1;
        is $c->get('bar'), $t_bar;
        isnt $c->get('baz'), $t_baz;
    }
};

done_testing;