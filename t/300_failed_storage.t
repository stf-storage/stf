use strict;
use Test::More;
use Plack::Test;
use HTTP::Request::Common qw(PUT HEAD GET DELETE POST);
use HTTP::Date;
use STF::Test;
use STF::Test qw(clear_queue);

use_ok "STF::Context";

# String::URandomとか使っても良いけど面倒くさい
my $random_string = sub {
    my @chars = ('a'..'z');
    join "", map { $chars[ rand @chars ] } 1..($_[0] || 8);
};

# PUT an object (success)
# GET an object (success)
# DELETE an entity, and INSERT a dummy one
# GET an object, and this should succeed, FAST
my $code = sub {
    my ($cb) = @_;
    my $res;
    my $bucket_name = $random_string->();
    my $object_name = $random_string->();

    $res = $cb->(
        PUT "http://127.0.0.1/$bucket_name"
    );
    if (! ok $res->is_success, "bucket creation request was successful") {
        diag $res->as_string;
    }

    $res = $cb->(
        PUT "http://127.0.0.1/$bucket_name/$object_name",
            "X-STF-Consistency" => 3,
            "Content" => $random_string->(1024)
    );

    for (1..5) {
        $res = $cb->(
            GET "http://127.0.0.1/$bucket_name/$object_name",
        );
        if (! ok $res->is_success, "GET is successful") {
            diag $res->as_string;
        }
    }

    my $context = STF::Context->bootstrap( config => "t/config.pl" ) ;
    my $dbh = $context->container->get('DB::Master');

    my $object = $dbh->selectrow_hashref( <<EOSQL, undef, $bucket_name, $object_name );
        SELECT o.* FROM object o
            JOIN bucket b ON o.bucket_id = b.id
            WHERE b.name = ? AND o.name = ?
EOSQL
    ok $object;

    my $cache_key  = [ 'storages_for', $object->{id} ];
    my $object_api = $context->container->get('API::Object');
    my $storages   = $object_api->cache_get( @$cache_key );
    $storages->[0]->[1] =~ s{^(http://[^/]+):\d+}{$1:99999999};

    $object_api->cache_set( $cache_key, $storages, 180 );
    if ( ! ok $object_api->cache_get( @$cache_key ), "sanity check" ) {
        diag "CACHE SET FOR storages_for.$object->{id} failed?!";
    }

    $res = $cb->(
        GET "http://127.0.0.1/$bucket_name/$object_name",
    );
    if (! ok $res->is_success, "GET is successful (first time)") {
        diag $res->as_string;
    }

    $res = $cb->(
        GET "http://127.0.0.1/$bucket_name/$object_name",
    );
    if (! ok $res->is_success, "GET is successful (second time)") {
        diag $res->as_string;
    }

    $storages = $object_api->cache_get( @$cache_key );
    if ( ! unlike $storages->[0]->[1], qr{:99999999}, "entities[0] shouldn't contain the broken entity" ) {
        diag explain $storages;
    }
};

clear_queue();
my $app = require "t/dispatcher.psgi";
test_psgi
    app => $app,
    client => $code,
;

done_testing;
