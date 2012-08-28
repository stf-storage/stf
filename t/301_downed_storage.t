use strict;
use Test::More;
use Plack::Test;
use HTTP::Request::Common qw(PUT HEAD GET POST);
use HTTP::Date;
use Scope::Guard ();
use STF::Test;
use STF::Test qw(clear_queue);
BEGIN {
    use_ok "STF::Constants",
        "STF_TRACE",
        "STORAGE_MODE_READ_WRITE",
        "STORAGE_MODE_TEMPORARILY_DOWN",
    ;
}

use_ok "STF::Context";

# String::URandomとか使っても良いけど面倒くさい
my $random_string = sub {
    my @chars = ('a'..'z');
    join "", map { $chars[ rand @chars ] } 1..($_[0] || 8);
};

# PUT an object (success)
# GET an object (success)
# Storage->update($id, mode => STORAGE_MODE_TEMPORARILY_DOWN );
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

    my $context = STF::Context->bootstrap() ;

    # XXX Find where this object belongs to
    my $container = $context->container;
    my $dbh = $container->get('DB::Master');
    my $object = $dbh->selectrow_hashref( <<EOSQL, undef, $bucket_name, $object_name );
        SELECT o.* FROM object o
            JOIN bucket b ON o.bucket_id = b.id
            WHERE b.name = ? AND o.name = ?
EOSQL
    ok $object;

    my $cluster_api = $container->get('API::StorageCluster');
    my $cluster = $cluster_api->load_for_object( $object->{id} );

    my @entities = $container->get('API::Entity')->search(
        {
            object_id => $object->{id},
        },
        {
            order_by => 'rand()'
        }
    );

    my $tracer;
    if (STF_TRACE) {
        $tracer = $container->get('Trace');

        # Clear the tracer so we know the values are fresh
        $tracer->clear();
    }
    $container->get('API::Storage')->update( $entities[0]->{storage_id}, {
        mode => STORAGE_MODE_TEMPORARILY_DOWN
    });
    my $guard = Scope::Guard->new(sub {
        $container->get('API::Storage')->update( $entities[0]->{storage_id}, {
            mode => STORAGE_MODE_READ_WRITE
        });
    });

    for (1..5) {
        $res = $cb->(
            GET "http://127.0.0.1/$bucket_name/$object_name",
        );
        if (! ok $res->is_success, "GET is successful") {
            diag $res->as_string;
        }
    }

    if (STF_TRACE) {
        # make sure that the storage cache was invalidated
        my $list = $tracer->dbh->selectall_arrayref(<<EOSQL, { Slice => {} }, "stf.object.get_any_valid_entity_url.invalidated_storage_cache");
            SELECT * FROM trace_log WHERE name = ?
EOSQL
        ok @$list > 0, "storage cache was invalidated @{[ scalar @$list ]} times (> 0)";
    }

    # This should work, and this should create an object in a different
    # cluster than the original one
    $res = $cb->(
        PUT "http://127.0.0.1/$bucket_name/$object_name",
            "X-STF-Consistency" => 3,
            "Content" => $random_string->(1024)
    );
    my $new_cluster = $cluster_api->load_for_object( $object->{id} );
    isnt $cluster->{id}, $new_cluster->{id}, "object is now in a different cluster";

    undef $guard;

    if (STF_TRACE) {
        $tracer->clear();
    }

    # Now we should be back to read-write
    for (1..5) {
        $res = $cb->(
            GET "http://127.0.0.1/$bucket_name/$object_name",
        );
        if (! ok $res->is_success, "GET is successful") {
            diag $res->as_string;
        }
    }
    
    if (STF_TRACE) {
        # make sure that the storage cache was invalidated
        my $list = $tracer->dbh->selectall_arrayref(<<EOSQL, { Slice => {} }, "stf.object.get_any_valid_entity_url.invalidated_storage_cache");
            SELECT * FROM trace_log WHERE name = ?
EOSQL
        ok @$list == 0, "storage cache was invalidated @{[ scalar @$list ]} times (== 0)";
    }

};

clear_queue();
my $app = require "t/dispatcher.psgi";
test_psgi
    app => $app,
    client => $code,
;

done_testing;
