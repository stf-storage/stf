use strict;
use Test::More;
use Plack::Test;
use HTTP::Request::Common qw(GET PUT);
use STF::Test qw(clear_queue);

use_ok "STF::Context";
use_ok "STF::Worker::ObjectHealth";
use_ok "STF::Worker::RepairObject";
use_ok "STF::Worker::Replicate";

clear_queue;

my $code = sub {
    my $cb = shift;

    my $IS_SCHWARTZ;
    my $context = STF::Context->bootstrap( config => "t/config.pl" );
    my $container = $context->container;
    {
        my $queue_dbh = $container->get( 'DB::Queue' );
        # crude way to figure out which check to perform
        my $list = $queue_dbh->selectall_arrayref( <<EOSQL, undef );
            SHOW TABLES
EOSQL
        $IS_SCHWARTZ = grep { $_->[0] eq 'job' } @$list;
    }
    note "Are we using Schwartz? => " . ($IS_SCHWARTZ ? "YES" : "NO");

    my $bucket = "repair";
    my $content = join ".", $$, time(), {}, rand();

    $cb->( PUT "http://127.0.0.1/$bucket" );
    # put a few items so the neighbor finding deal kicks in
    foreach my $i (1..10) {
        $cb->( PUT "http://127.0.0.1/$bucket/pre-test-$i",
            "X-STF-Consistency" => 2,
            "Content-Type"        => "text/plain",
            "Content"             => $content,
        );
    }

    $cb->( PUT "http://127.0.0.1/$bucket/test",
        "X-STF-Consistency" => 2,
        "Content-Type"        => "text/plain",
        "Content"             => $content,
    );

    # put a few items so the neighbor finding deal kicks in
    foreach my $i (1..10) {
        $cb->( PUT "http://127.0.0.1/$bucket/post-test-$i",
            "X-STF-Consistency" => 2,
            "Content-Type"        => "text/plain",
            "Content"             => $content,
        );
    }

    my $guard    = $container->new_scope();
    my $dbh      = $container->get( 'DB::Master' );
    my $entities = $dbh->selectall_arrayref( <<EOSQL, { Slice => {} }, $bucket, "test" );
        SELECT o.id, s.uri, o.internal_name
            FROM object o JOIN bucket b ON b.id = o.bucket_id
                          JOIN entity e ON o.id = e.object_id
                          JOIN storage s ON s.id = e.storage_id 
            WHERE b.name = ? AND o.name = ?
EOSQL

    # XXX Since we're doing consistent hashing underneath, we need to
    # delete the FIRST entity in order to be *sure* that repair++ is
    # called in the background
    my %entities = map {
        my $uri = "$_->{uri}/$_->{internal_name}";
        ( $uri => { %$_, 
            uri => $uri,
            hash => Digest::MurmurHash::murmur_hash($uri),
        } )
    } @$entities;

    my $furl = $container->get('Furl');
    my ($target)= map { $entities{$_} } sort { $entities{$a}->{hash} <=> $entities{$b}->{hash} } keys %entities;

    $furl->delete( $target->{uri} );

    my $pattern = "t/store*/$target->{internal_name}";
    {
        my @files = glob( $pattern );
        is scalar @files, 1, "Should be 1 files";
    }

    # Keep requesting until the queue contains at least one repair_object
    # job in it
    my $queue_dbh = $container->get( 'DB::Queue' );
    my $found_repair = 0;
    for (1..100) {
        $cb->( GET "http://127.0.0.1/$bucket/test" );

        my $object;
        if ( $IS_SCHWARTZ ) {
            $object = $queue_dbh->selectrow_array( <<EOSQL, undef, $target->{id}, "STF::Worker::RepairObject::Proxy" );
                SELECT job.* FROM job JOIN funcmap ON job.funcid = funcmap.funcid WHERE job.arg = ? AND funcmap.funcname = ?
EOSQL
        } else {
            $object = $queue_dbh->selectrow_array( <<EOSQL, undef, $target->{id});
                SELECT * FROM queue_repair_object WHERE args = ?
EOSQL
        }

        if ($object) {
            $found_repair = 1;
            last;
        }
    }

    if( !ok $found_repair, "Found a repair request in queue") {
        fail ("Can't continue without finding a repair request. Abort" );
        return;
    }

    eval {
        local $SIG{ALRM} = sub { die "RepairObject timeout" };
        alarm(5);
        my $worker = STF::Worker::RepairObject->new(
            container => $context->container,
            max_works_per_child => 1,
            breadth => 2,
        );
        $worker->work;
    };
    if ($@) {
        fail "Error running RepairObject worker: $@";
    }
    alarm(0);

    note "At this point, the object is fixed.";

    { # check files
        my @files = glob( $pattern );
        is scalar @files, 2, "Should be 2 files";
    }

    { # check entities
        my $dbh      = $container->get( 'DB::Master' );
        my $entities = $dbh->selectall_arrayref( <<EOSQL, { Slice => {} }, $bucket, "test" );
            SELECT o.id, s.uri, o.internal_name
                FROM object o JOIN bucket b ON b.id = o.bucket_id
                              JOIN entity e ON o.id = e.object_id
                              JOIN storage s ON s.id = e.storage_id 
                WHERE b.name = ? AND o.name = ?
EOSQL
        is scalar @$entities, 2, "Should be 2 entities";
    }

    note "Now we should check out if the object health worker received any inputs";

    my $object_health_queue_size;
    if ( $IS_SCHWARTZ ) {
        ($object_health_queue_size) = $queue_dbh->selectrow_array( <<EOSQL, undef, "STF::Worker::ObjectHealth::Proxy" );
            SELECT count(*) FROM job JOIN funcmap ON job.funcid = funcmap.funcid WHERE funcmap.funcname = ?
EOSQL
    } else {
        ($object_health_queue_size) = $queue_dbh->selectrow_array( <<EOSQL );
            SELECT count(*) FROM queue_object_health
EOSQL
    }

    if ( ok $object_health_queue_size > 0, "object_health_queue_size is > 0" ) {
        # run the repair worker a bit longer
        eval {
            local $SIG{ALRM} = sub {
                die "RepairObject worker timed out";
            };
            alarm(5);
            my $worker = STF::Worker::ObjectHealth->new(
                container => $context->container,
                max_works_per_child => 1,
            );
            $worker->work;
        };
        if ($@) {
            fail "Error running RepairObject Worker: $@";
        }
        alarm(0);
    }

    my $object_health_queue_size_after;
    if ( $IS_SCHWARTZ ) {
        ($object_health_queue_size_after) = $queue_dbh->selectrow_array( <<EOSQL, undef, "STF::Worker::RepairObject::Proxy" );
            SELECT count(*) FROM job JOIN funcmap ON job.funcid = funcmap.funcid WHERE funcmap.funcname = ?
EOSQL
    } else {
        ($object_health_queue_size_after) = $queue_dbh->selectrow_array( <<EOSQL );
            SELECT count(*) FROM queue_repair_object
EOSQL
    }

    ok $object_health_queue_size_after < $object_health_queue_size, "queue size has not increased (before=$object_health_queue_size, after=$object_health_queue_size_after)";
};
my $app = require "t/dispatcher.psgi";
test_psgi 
    app => $app,
    client => $code
;

done_testing;