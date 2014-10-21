# Mark storage as crashed. Contents in the crashed storage should be
# migrated to a different cluster/storage

use strict;
use Test::More;
use Plack::Test;
use HTTP::Request::Common qw(PUT HEAD GET POST);
use HTTP::Date;
use Scope::Guard ();
use STF::Test;
use STF::Test qw(clear_objects clear_queue);
BEGIN {
    use_ok "STF::Constants",
        "STF_TRACE",
        "STORAGE_MODE_REPAIR",
        "STORAGE_MODE_READ_WRITE",
        "STORAGE_MODE_TEMPORARILY_DOWN",
    ;
}

use_ok "STF::Context";
use_ok "STF::Worker::Replicate";
use_ok "STF::Worker::RepairStorage";
use_ok "STF::Worker::RepairObject";

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
    my $context = STF::Context->bootstrap() ;
    my $container = $context->container;
    my $storage_api = $container->get('API::Storage');
    my $cluster_api = $container->get('API::StorageCluster');
    my $dbh = $container->get('DB::Master');
    my $tracer;
    if (STF_TRACE) {
        $tracer = $container->get('Trace');

        # Clear the tracer so we know the values are fresh
        $tracer->clear();
    }

    # create a bucket...
    $res = $cb->(
        PUT "http://127.0.0.1/$bucket_name"
    );
    if (! ok $res->is_success, "bucket creation request was successful") {
        diag $res->as_string;
    }

    # We need to have enough entities in a given storage.
    # choose a cluster, and create objects until we create
    # enough objects in that given cluster

    my $find_object_id_sth = $dbh->prepare( <<EOSQL );
        SELECT o.* FROM object o
            JOIN bucket b ON o.bucket_id = b.id
            WHERE b.name = ? AND o.name = ?
EOSQL
    my ($cluster) = $cluster_api->load_writable();
    my @object_names;
    my $total_objects = 0;
    do {
        my $object_name = $random_string->();
        $res = $cb->(
            PUT "http://127.0.0.1/$bucket_name/$object_name",
                "Content" => $random_string->(1024)
        );
        my ($object_id) = $dbh->selectrow_array($find_object_id_sth, undef, $bucket_name, $object_name);
        ok $object_id, "Found object ID of $bucket_name/$object_name ($object_id)";
        my $new_cluster = $cluster_api->calculate_for_object($object_id);
        if ($new_cluster->{id} eq $cluster->{id}) {
            push @object_names, $object_name;
        }
        $total_objects++;
    } while (@object_names < 10);

    {
        my $worker = STF::Worker::Replicate->new(
            container => $container,
            max_works_per_child => $total_objects,
        );
        $worker->work;
    }


    # At this point we know there are at least 10 objects in this cluster
    # Now change the mode of a storage in this cluster to NEED REPAIR.

    my ($storage) = $storage_api->search({
        cluster_id => $cluster->{id}
    });

    $storage_api->update( $storage->{id}, {
        mode => STORAGE_MODE_REPAIR
    });
    $storage = $storage_api->lookup($storage->{id});
    my $guard = Scope::Guard->new(sub {
        $container->get('API::Storage')->update( $storage->{id}, {
            mode => STORAGE_MODE_READ_WRITE
        });
    });

    # Fire up the RepairStorage worker, and see it insert all the entities
    # in the worker

    {
        my $worker = STF::Worker::RepairStorage->new(
            container => $container,
            max_works_per_child => 1,
        );
        $worker->work;
    }

    # At this point we now know that the entities in the storage are
    # set to be repaired. This repair should actually force the
    # entities to be migrated to a different cluster

    {
        # XXX need to find out exactly how many requests we
        # should be processing in order not to block
        my $queue_api = $container->get('API::Queue');
        my $count     = $queue_api->size( "repair_object" );
        my $worker = STF::Worker::RepairObject->new(
            container => $container,
            max_works_per_child => $count,
        );
        $worker->work;
    }

    # Repair done!
    # now check that the objects are in a different cluster from the
    # original one that they were created in
    foreach my $object_name (@object_names) {
        my ($object_id) = $dbh->selectrow_array($find_object_id_sth, undef, $bucket_name, $object_name);
        my $my_cluster = $cluster_api->calculate_for_object($object_id);
        is $my_cluster->{id}, $cluster->{id}, "object $bucket_name/$object_name should be in the same cluster";
    }

    if (STF_TRACE) {
        $tracer->clear();
    }

    # Oh, and make sure they are readable, while we're at it
    foreach my $object_name ( @object_names ) {
        $res = $cb->(
            GET "http://127.0.0.1/$bucket_name/$object_name",
        );
        if (! ok $res->is_success, "GET is successful for $bucket_name/$object_name") {
            diag $res->as_string;
        }
    }

    # We should also check that cache has been invalidated when
    # we fetched these
    if (STF_TRACE) {
        # make sure that the storage cache was invalidated
        my $list = $tracer->dbh->selectall_arrayref(<<EOSQL, { Slice => {} }, "stf.object.get_any_valid_entity_url.invalidated_storage_cache");
            SELECT * FROM trace_log WHERE name = ?
EOSQL
        is scalar @$list, scalar @object_names, "storage cache was invalidated @{[ scalar @$list ]} times (> @{[ scalar @object_names ]})";
    }
};

clear_queue();
clear_objects();
my $app = require "t/dispatcher.psgi";
test_psgi
    app => $app,
    client => $code,
;

done_testing;

