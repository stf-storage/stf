use strict;
use Test::More;
use Plack::Test;
use HTTP::Request::Common qw(PUT);
use STF::Test qw(clear_queue);
use STF::Constants qw(:storage);
use Guard;

use_ok "STF::Context";
use_ok "STF::Worker::RepairObject";
use_ok "STF::Worker::RetireStorage";
use_ok "STF::Worker::Replicate";

clear_queue;

my $code = sub {
    my $cb = shift;
    my $context = STF::Context->bootstrap( config => "t/config.pl" );
    my $container = $context->container;

    my $bucket = "retire";
    my $content = join ".", $$, time(), {}, rand();

    $cb->( PUT "http://127.0.0.1/$bucket" );
    $cb->( PUT "http://127.0.0.1/$bucket/test",
        "X-Replication-Count" => 2,
        "Content-Type"        => "text/plain",
        "Content"             => $content,
    );

    {
        my $worker = STF::Worker::Replicate->new(
            container => $context->container,
            max_works_per_child => 1,
        );
        $worker->work;
    }

    # choose 1 storage to be "retired"
    my $guard = $container->new_scope();
    my $dbh = $container->get( 'DB::Master' );
    my $storages = $dbh->selectall_arrayref( <<EOSQL, { Slice => {} } );
        SELECT s.* FROM storage s
EOSQL
    my $retired  = $storages->[0];

    $dbh->do( <<EOSQL, undef, STORAGE_MODE_RETIRE, $retired->{id} );
        UPDATE storage SET mode = ? WHERE id = ?
EOSQL
    my $mode_guard = guard {
        eval {
            $dbh->do( <<EOSQL, undef, STORAGE_MODE_READ_WRITE, $retired->{id} );
                UPDATE storage SET mode = ? WHERE id = ?
EOSQL
        }
    };

    {
        my $worker = STF::Worker::RetireStorage->new(
            container => $context->container,
            max_works_per_child => 1,
        );
        $worker->work;
    }

    {
        my $worker = STF::Worker::RepairObject->new(
            container => $context->container,
            max_works_per_child => 1,
        );
        $worker->work;
    }


    # check that entity count is back
    my $entities = $dbh->selectall_arrayref( <<EOSQL, { Slice => {} }, $bucket, "test" );
        SELECT e.* FROM entity e JOIN object o ON e.object_id  = o.id
                                 JOIN bucket b ON o.bucket_id  = b.id
            WHERE b.name = ? AND o.name = ?
EOSQL
    is scalar @$entities, 2, "Should be 2 entities";
    foreach my $entity (@$entities) {
        isnt $entity->{storage_id}, $retired->{id}, "entities are not in the reitred storage";
    }
};

test_psgi
    app => do "t/dispatcher.psgi",
    client => $code,
;

done_testing;
