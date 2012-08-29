use strict;
use Test::More;
use Plack::Test;
use HTTP::Request::Common qw(GET PUT);
use STF::Test qw(clear_objects clear_queue);

use_ok "STF::Context";
use_ok "STF::Worker::RepairObject";
use_ok "STF::Worker::Replicate";

clear_queue;
clear_objects;

my $code = sub {
    my $cb = shift;

    my $IS_SCHWARTZ;
    my $context = STF::Context->bootstrap();
    my $container = $context->container;

    my $queue_type = $ENV{STF_QUEUE_TYPE}  || 'Q4M';
    note "Using queue type $queue_type";

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

    {
        my $guard = $container->new_scope();
        my $worker = STF::Worker::Replicate->new(
            container => $container,
            max_works_per_child => 21,
        );
        $worker->work;
    }

    my $guard    = $container->new_scope();
    my $dbh      = $container->get( 'DB::Master' );

    my $entities_before = $dbh->selectall_arrayref( <<EOSQL, { Slice => {} }, $bucket, "test" );
        SELECT o.id, s.uri, o.internal_name
            FROM object o JOIN bucket b ON b.id = o.bucket_id
                          JOIN entity e ON o.id = e.object_id
                          JOIN storage s ON s.id = e.storage_id 
            WHERE b.name = ? AND o.name = ?
EOSQL

    { # Ideally we need to check that nothing happened here (none of
      # these need repair), but I haven't done it here. TODO
        my %objects = map { ($_->{id} => 1) } @$entities_before;
        my $work = 0;
        foreach my $object_id ( keys %objects ) {
            $work++;
            $container->get( 'API::Queue' )->enqueue( repair_object => $object_id );
        }

        eval {
            local $SIG{ALRM} = sub { die "RepairObject timeout" };
            alarm(5);
            my $worker = STF::Worker::RepairObject->new(
                container => $context->container,
                max_works_per_child => $work,
                breadth => 2,
            );
            $worker->work;
        };
        if ($@) {
            fail "Error running RepairObject worker: $@";
        }
        alarm(0);
    }

    my $entities_after = $dbh->selectall_arrayref( <<EOSQL, { Slice => {} }, $bucket, "test" );
        SELECT o.id, s.uri, o.internal_name
            FROM object o JOIN bucket b ON b.id = o.bucket_id
                          JOIN entity e ON o.id = e.object_id
                          JOIN storage s ON s.id = e.storage_id 
            WHERE b.name = ? AND o.name = ?
EOSQL

    is scalar @$entities_after, scalar @$entities_before, "no repair happend";

    # XXX Since we're doing consistent hashing underneath, we need to
    # delete the FIRST entity in order to be *sure* that repair++ is
    # called in the background
    my %entities = map {
        my $uri = "$_->{uri}/$_->{internal_name}";
        ( $uri => { %$_, 
            uri => $uri,
            hash => Digest::MurmurHash::murmur_hash($uri),
        } )
    } @$entities_after;

    my $furl = $container->get('Furl');
    my @sorted = 
        map { $entities{$_} }
        sort { $entities{$a}->{hash} <=> $entities{$b}->{hash} }
        keys %entities
    ;

    my $target = $sorted[0];
    my $pattern = "t/storage*/$target->{internal_name}";

    my @before;
    {
        @before = glob( $pattern );

        note "Deleting $target->{uri} from storage to mimic a corrupt entity";
        $furl->delete( $target->{uri} );

        my @files = glob( $pattern );
        is scalar @files, scalar @before - 1, "Should be " . scalar @before - 1 . " files (got " . scalar @files . ")";
    }

    # Keep requesting until the queue contains at least one repair_object
    # job in it
    my $found_repair = 0;
    my $queue_dbh = $container->get('DB::Queue');
    for (1..100) {
        $cb->( GET "http://127.0.0.1/$bucket/test" );

        my $object;
        if ( $queue_type eq 'Schwartz' ) {
            $object = $queue_dbh->selectrow_array( <<EOSQL, undef, $target->{id}, "STF::Worker::RepairObject::Proxy" );
                SELECT job.* FROM job JOIN funcmap ON job.funcid = funcmap.funcid WHERE job.arg = ? AND funcmap.funcname = ?
EOSQL
        } elsif ( $queue_type eq 'Resque' ) {
            my @jobs = $queue_dbh->peek('repair_object');
            foreach my $job (@jobs) {
                if ($job->args->[0] eq $target->{id}) {
                    $object = $job;
                    last;
                }
            }
        } elsif ( $queue_type eq 'Redis') {
            my @jobs = $queue_dbh->lrange('repair_object', 0, 0);
            foreach my $job (@jobs) {
                if ($job =~ /\b$target->{id}\b/) {
                    $object = $job;
                    last;
                }
            }
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
        is scalar @files, scalar @before, "Should be " . scalar @before . " files in $pattern";
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
        is scalar @$entities, scalar @before, "Should be " . scalar @before . " entities";
    }

    note "alright, it worked for the most normal case of storages crashing. Howabout corrupted files?";
    my ($broken_file) = sort { rand } glob $pattern;
    {
        open my $fh, '>', $broken_file;
        print $fh "garbage!";
        close $fh;
    }

    my $bucket_object = $container->get('API::Bucket')->lookup_by_name( $bucket );
    my $object_id = $container->get('API::Object')->find_object_id( {
        bucket_id => $bucket_object->{id},
        object_name => "test",
    } );
    $container->get('API::Queue')->enqueue( repair_object => $object_id );

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

    {
        open my $fh, '<', $broken_file;
        my $content = do { local $/; <$fh> };
        isnt $content, "garbage!", "Object should be fixed";
    }
    note "At this point, the object is fixed.";

    { # check files
        my @files = glob( $pattern );
        is scalar @files, scalar @before, "Should be " . scalar @before . " files in $pattern";
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
        is scalar @$entities, scalar @before, "Should be " . scalar @before . " entities";
    }

    # XXX Need to test case when entities are intact, but are in the
    # wrong cluster.
    {
        my $dbh      = $container->get( 'DB::Master' );
        my $clusters = $dbh->selectall_arrayref( <<EOSQL, { Slice => {} } );
            SELECT * FROM storage_cluster
EOSQL
        my %shiftmap;
        foreach my $i (0.. (scalar(@$clusters) - 1)) {
            $shiftmap{ $clusters->[$i]->{id} } = $clusters->[$i - 1]->{id};
        }

        my $storage_api = $container->get('API::Storage');
        my @storages    = $storage_api->search({});
        foreach my $storage ( @storages ) {
            note "Mapping $storage->{id} from $storage->{cluster_id} to $shiftmap{ $storage->{cluster_id} } to swap";
            $storage_api->update( $storage->{id}, {
                cluster_id => $shiftmap{ $storage->{cluster_id} }
            } );
        }

        # now we should have mismatch between which cluster the object thinks
        # it is stored

        my $work = 0;
        my %objects = map { ($_->{id} => 1) } @$entities_after;
        foreach my $object_id ( keys %objects ) {
            my $map = $dbh->selectrow_hashref( <<EOSQL, undef, $object_id );
                SELECT * FROM object_cluster_map WHERE object_id = ?
EOSQL
            my $cluster_id = $dbh->selectall_arrayref( <<EOSQL, { Slice => {} }, $object_id );
                SELECT s.cluster_id FROM storage s JOIN entity e ON
                    s.id = e.storage_id
                    WHERE e.object_id = ?
EOSQL
            isnt $cluster_id->[0]->{cluster_id}, $map->{cluster_id}, "we have a mismatch";
            $work++;
            $container->get( 'API::Queue' )->enqueue( repair_object => $object_id );
        }

        eval {
            local $SIG{ALRM} = sub { die "RepairObject timeout" };
            alarm(5);
            my $worker = STF::Worker::RepairObject->new(
                container => $context->container,
                max_works_per_child => $work,
                breadth => 2,
            );
            $worker->work;
        };
        if ($@) {
            fail "Error running RepairObject worker: $@";
        }
        alarm(0);

        foreach my $object_id ( keys %objects ) {
            my $map = $dbh->selectrow_hashref( <<EOSQL, undef, $object_id );
                SELECT * FROM object_cluster_map WHERE object_id = ?
EOSQL
            my $cluster_id_list = $dbh->selectall_arrayref( <<EOSQL, { Slice => {} }, $object_id );
                SELECT s.cluster_id FROM storage s JOIN entity e ON
                    s.id = e.storage_id
                    WHERE e.object_id = ?
EOSQL
            foreach my $cluster_id (@$cluster_id_list) {
                is $cluster_id->{cluster_id}, $map->{cluster_id}, "we DON'T have a mismatch";
            }
        }
    }
};
my $app = require "t/dispatcher.psgi";
test_psgi 
    app => $app,
    client => $code
;

done_testing;