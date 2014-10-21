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
        # XXX
        # 1. Choose random objects
        # 2. Move them out of their current clusters
        # 3. Send them to repair
        my %expect_map;
        my @clusters = $container->get('API::StorageCluster')->search({});
        my @objects = $container->get('API::Object')->search(
            {},
            { limit => 10, order_by => 'rand()' }
        );

        my $entity_api = $container->get('API::Entity');
        my $storage_api = $container->get('API::Storage');
        foreach my $object (@objects) {
            my $registered = $entity_api->load_majority_cluster_for($object->{id});
            my $new_cluster_id;
            foreach my $cluster (sort { rand } @clusters) {
                next if $cluster->{id} == $registered->{id};
                $new_cluster_id = $cluster->{id};
                last;
            }

            my @new_storages = $storage_api->search({
                cluster_id => $new_cluster_id,
            });

            my @entities = $entity_api->search({
                object_id => $object->{id}
            });
            my @old_storages = 
                map { $storage_api->lookup($_->{storage_id}) }
                @entities
            ;
            $expect_map{$object->{id}} = \@old_storages;

            my $content = $entity_api->fetch_content_from_any({
                object => $object
            });

            note "Moving object $object->{id} from storages @{[ map { $_->{id} } @old_storages ]} to @{[ map { $_->{id} } @new_storages ]}";
            foreach my $storage (@new_storages) {
                $entity_api->store({
                    object => $object,
                    storage => $storage,
                    content => $content
                });
            }
            $entity_api->remove({
                object => $object,
                storages => \@old_storages,
            });
        }

        # now we should have mismatch between which cluster the object thinks
        # it is stored
        foreach my $object (@objects) {
            $container->get('API::Queue')->enqueue(repair_object => $object->{id});
        }

        eval {
            local $SIG{ALRM} = sub { die "RepairObject timeout" };
            alarm(5);
            my $worker = STF::Worker::RepairObject->new(
                container => $context->container,
                max_works_per_child => scalar @objects,
            );
            $worker->work;
        };
        if ($@) {
            fail "Error running RepairObject worker: $@";
        }
        alarm(0);

        foreach my $object ( @objects ) {
            # Make sure that this object is stored in the original storages
            my @entities = $entity_api->search({
                object_id => $object->{id}
            });
            my @storages = 
                sort { $a <=> $b }
                map  { $_->{id} }
                map  { $storage_api->lookup($_->{storage_id}) }
                @entities
            ;
            my @expected = sort { $a <=> $b } map { $_->{id} } @{$expect_map{$object->{id}}};
            if (! is_deeply(\@storages, \@expected, "Entities have been restored!")) {
                diag explain \@entities;
                diag explain \%expect_map;
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