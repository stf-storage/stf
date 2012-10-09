use strict;
use Test::More;
use STF::Test;

use_ok "STF::Context";

my $context = STF::Context->bootstrap() ;
my $api = $context->get( 'API::Bucket' );

# String::URandomとか使っても良いけど面倒くさい
my $random_string = sub {
    my @chars = ('a'..'z');
    join "", map { $chars[ rand @chars ] } 1..($_[0] || 8);
};

ok($api);

my %buckets = map { ($_->{id} => 1) } $api->search();
my $id;

do {
    $id = int(rand() * 1_000_000);
} while ( $buckets{$id} );

$api->create( {
    id => $id,
    name => $random_string->()
});

ok $api->lookup($id), "lookup bucket OK";
$api->delete( {
    id => $id,
    recursive => 0,
} );

{
    my $got = $api->lookup($id);
    if (! ok !$got, "lookup deleted bucket should fail") {
        diag explain $got;
    }
}

done_testing;