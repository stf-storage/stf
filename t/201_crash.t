use strict;
use Test::More;
BEGIN {
    plan skip_all => "This test is not really relevant anymore, as the crash recover worker is no longer needed";
}

use Plack::Test;
use HTTP::Request::Common qw(PUT DELETE);
use STF::Test qw(clear_queue);
use STF::Constants qw(:storage);
use Guard;

use_ok "STF::Context";
use_ok "STF::Worker::RecoverCrash";
use_ok "STF::Worker::RepairObject";
use_ok "STF::Worker::Replicate";

clear_queue;
my $random_string = sub {
    my @chars = ('a'..'z');
    join "", map { $chars[ rand @chars ] } 1..($_[0] || 8);
};

my $code = sub {
    my $cb = shift;
    my $context = STF::Context->bootstrap( config => "t/config.pl" );
    my $container = $context->container;

    my $guard = $container->new_scope();
    my $dbh = $container->get( 'DB::Master' );

    # Clean up the database so that we don't get bogus errors
    # (This should really be done in the original test that
    # broke the consistency, damnit)
    {
        my $list = $dbh->selectall_arrayref( <<EOSQL, { Slice => {} } );
            SELECT * FROM entity
EOSQL
        foreach my $entity (@$list) {
            my $object = $dbh->selectrow_hashref( <<EOSQL, undef, $entity->{object_id} );
                SELECT * FROM object WHERE id = ?
EOSQL
            if (! $object) {
                $dbh->do( <<EOSQL, undef, $entity->{object_id} );
                    DELETE FROM entity WHERE object_id = ?
EOSQL
            }
        }
    }

    my $bucket_name = $random_string->();
    my $object_name = $random_string->(32);
    my $content     = $random_string->(128);

    $cb->( PUT "http://127.0.0.1/$bucket_name" );
    $cb->( PUT "http://127.0.0.1/$bucket_name/$object_name",
        "X-Replication-Count" => 2,
        "Content-Type"        => "text/plain",
        "Content"             => $content,
    );

    # find me the bucket and the object
    my $bucket    = $container->get('API::Bucket')->lookup_by_name( $bucket_name );
    my $object_id = $container->get('API::Object')->find_object_id({
        bucket_id => $bucket->{id},
        object_name => $object_name,
    });
    my $object = $container->get('API::Object')->lookup( $object_id );
    my $cluster = $container->get('API::StorageCluster')->load_for_object( $object->{id} );

diag explain $cluster;

    {
        my $worker = STF::Worker::Replicate->new(
            container => $context->container,
            max_works_per_child => 1,
        );
        $worker->work;
    }

    # choose 1 storage to be "crashed"
    my $storages = $dbh->selectall_arrayref( <<EOSQL, { Slice => {} } );
        SELECT s.* FROM storage s
EOSQL
    my $crashed;
    my $count;
    foreach my $storage (@$storages) {
        ($count) = $dbh->selectrow_array( <<EOSQL, undef, $storage->{id} );
            SELECT count(*) FROM entity WHERE storage_id = ?
EOSQL
        if ($count > 0) {
            note "Storage $storage->{id} has $count entities. Choosing this as our crashed storage";
            $crashed = $storage;
            last;
        }
    }

    $dbh->do( <<EOSQL, undef, STORAGE_MODE_CRASH, $crashed->{id} );
        UPDATE storage SET mode = ? WHERE id = ?
EOSQL
    my $mode_guard = guard {
        eval {
            $dbh->do( <<EOSQL, undef, STORAGE_MODE_READ_WRITE, $crashed->{id} );
                UPDATE storage SET mode = ? WHERE id = ?
EOSQL
        }
    };

    {
        my $worker = STF::Worker::RecoverCrash->new(
            container => $context->container,
            max_works_per_child => 1,
        );
        $worker->work;
    }

    {
        my $worker = STF::Worker::RepairObject->new(
            container => $context->container,
            max_works_per_child => $count,
        );
        $worker->work;
    }

    # check that entity count is back
    my $entities = $dbh->selectall_arrayref( <<EOSQL, { Slice => {} }, $bucket_name, $object_name );
        SELECT e.* FROM entity e JOIN object o ON e.object_id  = o.id
                                 JOIN bucket b ON o.bucket_id  = b.id
            WHERE b.name = ? AND o.name = ?
EOSQL
    my ($storage_count) = $dbh->selectrow_array( <<EOSQL, undef, $cluster->{id} );
        SELECT COUNT(*) FROM storage WHERE cluster_id = ?
EOSQL

    is scalar @$entities, $storage_count, "Should be $storage_count entities";
    foreach my $entity (@$entities) {
        isnt $entity->{storage_id}, $crashed->{id}, "entities are not in the crashed storage";
    }
};

test_psgi
    app => do "t/dispatcher.psgi",
    client => sub {
        my $cb = shift;
        $code->($cb);
    }
;

done_testing;
