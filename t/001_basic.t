use strict;
use Cwd ();
use Digest::MD5 qw(md5_hex);
use Test::More;
use Plack::Test;
use Guard ();
use HTTP::Request::Common qw(PUT HEAD GET DELETE POST);
use HTTP::Date;
use STF::Constants qw(
    STF_ENABLE_OBJECT_META 
    STORAGE_CLUSTER_MODE_READ_ONLY STORAGE_CLUSTER_MODE_READ_WRITE
    STORAGE_MODE_TEMPORARILY_DOWN STORAGE_MODE_READ_WRITE
);
use STF::Test qw(clear_queue random_string);

use_ok "STF::Context";
use_ok "STF::Worker::DeleteBucket";
use_ok "STF::Worker::DeleteObject";
use_ok "STF::Worker::Replicate";

my $create_data = sub {
    my $chunks = shift;
    my $content;
    my $md5 = Digest::MD5->new;
    for (1..$chunks) {
        my $piece = random_string(1024);
        $md5->add($piece);
        $content .= $piece;
    }
    return ($content, $md5->hexdigest);
};

my $get_entity_count = sub {
    my ($dbh, $bucket_name, $object_name) = @_;
    my ($e_count) = $dbh->selectrow_array( <<EOSQL, undef, $bucket_name, $object_name);
        SELECT count(*) FROM entity e
            JOIN object o ON e.object_id = o.id 
            JOIN bucket b ON o.bucket_id = b.id
            WHERE b.name = ? AND o.name = ?
EOSQL
    return $e_count;
};

my $code = sub {
    my ($chunks, $cb) = @_;
    my $res;
    my $bucket_name = random_string();
    my $object_name = random_string();

    $res = $cb->(
        PUT "http://127.0.0.1/$bucket_name"
    );
    if (! ok $res->is_success, "bucket creation request was successful") {
        diag $res->as_string;
    }

    $res = $cb->(
        HEAD "http://127.0.0.1/$bucket_name/$object_name"
    );
    if ( ! ok ! $res->is_success, "HEAD before storing should fail" ) {
        diag $res->as_string;
    }

    $res = $cb->(
        GET "http://127.0.0.1/$bucket_name/$object_name"
    );
    if ( ! ok ! $res->is_success, "GET before storing should fail" ) {
        diag $res->as_string;
    }

    # 1K is the base unit. 1K = 1024 chars. 
    my @chars = ( 'a'..'z', 'A'..'Z', 0..9 );

    my ($content, $content_hash) = $create_data->($chunks);

    note "PUT to /$bucket_name/$object_name, replication count = 3";

    $res = $cb->(
        PUT "http://127.0.0.1/$bucket_name/$object_name",
            "X-Replication-Count" => 2,
            "Content-Type"        => "text/plain",
            "Content"             => $content,
    );
    if (! ok $res->is_success, "object creation request was successful") {
        diag $res->as_string;
    }

    my $context = STF::Context->bootstrap( config => "t/config.pl" ) ;

    { # find object ID and such
        my $guard = $context->container->new_scope();
        my $dbh = $context->container->get('DB::Master');

        my $object = $dbh->selectrow_hashref( <<EOSQL, undef, $bucket_name, $object_name );
            SELECT o.* FROM object o
                JOIN bucket b ON o.bucket_id = b.id
                WHERE b.name = ? AND o.name = ?
EOSQL
        ok $object, "found object matching $bucket_name + $object_name";

        if ( STF_ENABLE_OBJECT_META ) {
            my $meta = $dbh->selectrow_hashref( <<EOSQL, undef, $object->{id} );
                SELECT * FROM object_meta WHERE object_id = ?
EOSQL
            is $meta->{hash}, $content_hash, "content has is properly stoerd";
        }

        my $cluster = $context->container->get('API::StorageCluster')->load_for_object( $object->{id} );
        note "object $object->{id} is in cluster $cluster->{id}";

        # Make sure that only a double write (entity = 2) happend before
        # the replication worker arrives
        is $get_entity_count->( $dbh, $bucket_name, $object_name ), 2, "We have exactly 2 entities";

        my ($storage_count) = $dbh->selectrow_array(<<EOSQL, undef, $cluster->{id});
            SELECT COUNT(*) FROM storage WHERE cluster_id = ?
EOSQL

        {
            my $guard = $context->container->new_scope();
            my $dbh = $context->container->get('DB::Queue');
            my $worker = STF::Worker::Replicate->new(
                container => $context->container,
                max_works_per_child => 1,
            );
            $worker->work;
        }

        # Check that we have exactly $storage_count entities
        is $get_entity_count->($dbh, $bucket_name, $object_name), $storage_count, "After replication, there are exactly $storage_count entities created by the worker";
    }

    # GET / HEAD multiple times to make sure no stupid caching errors exist
    foreach my $i ( 1..2) {
        note "HEAD /$bucket_name/$object_name BEFORE storing ($i)";
        $res = $cb->(
            HEAD "http://127.0.0.1/$bucket_name/$object_name",
        );
        if (! ok $res->is_success, "HEAD is successful") {
            diag $res->as_string;
        } else {
            # should not contain X-Reproxy-URL
            if (! ok ! $res->header('X-Reproxy-URL'), "X-Reproxy-URL should not exist in response (HEAD BEFORE storing)" ) {
                diag $res->as_string;
            }
        }

        note "GET /$bucket_name/$object_name BEFORE storing ($i)";
        $res = $cb->(
            GET "http://127.0.0.1/$bucket_name/$object_name",
        );
        if (! ok $res->is_success, "GET is successful") {
            diag $res->as_string;
        } else {
            if (! is md5_hex($res->content), $content_hash, "content matches") {
                diag $res->as_string;
            }

            # should not contain X-Reproxy-URL
            if (! ok ! $res->header('X-Reproxy-URL'), "X-Reproxy-URL should not exist in response (GET BEFORE storing)" ) {
                diag $res->as_string;
            }
        }
    }

    {
        # GET / HEAD using If-Modified-Since
        $res = $cb->( HEAD "http://127.0.0.1/$bucket_name/$object_name" );

        my $last_modified = $res->header('Last-Modified');
        note "GET /$bucket_name/$object_name with IMS=$last_modified";
        $res = $cb->( HEAD "http://127.0.0.1/$bucket_name/$object_name", 'If-Modified-Since' => $last_modified );
        is $res->code, 304, "Got a 304";
    }

    # GET / HEAD non-existent urls
    foreach my $i ( 1..2) {
        note "HEAD /$bucket_name/$object_name-nonexistent ($i)";
        $res = $cb->(
            HEAD "http://127.0.0.1/$bucket_name/$object_name-nonexistent",
        );
        if (! is $res->code, 404, "HEAD should fail with 404") {
            diag $res->as_string;
        }

        note "GET /$bucket_name/$object_name ($i)";
        $res = $cb->(
            GET "http://127.0.0.1/$bucket_name/$object_name-nonexistent",
        );
        if (! is $res->code, 404, "GET should fail with 404") {
            diag $res->as_string;
        }
    }

    {
        # check proper clustering: basically make a storage not writable,
        # and then make sure that repeated writes to STF keeps writing to
        # the cluster that's alive

        my $cluster_api = $context->container->get('API::StorageCluster');
        my $storage_api = $context->container->get('API::Storage');
        my @storages    = $storage_api->search();
        my $broken      = $storages[ rand @storages ];

        $storage_api->update( $broken->{id}, { mode => STORAGE_MODE_TEMPORARILY_DOWN });
        $cluster_api->update( $broken->{cluster_id}, { mode => STORAGE_CLUSTER_MODE_READ_ONLY } );
       my $guard = Guard::guard(sub {
            $storage_api->update( $broken->{id}, { mode => STORAGE_MODE_READ_WRITE } );
            $cluster_api->update( $broken->{cluster_id}, { mode => STORAGE_CLUSTER_MODE_READ_WRITE } );
        } );

        my $dbh = $context->container->get('DB::Master');
        my @objects = map { random_string() } 1..30;
        foreach my $a_object_name ( @objects ) {
            $res = $cb->( PUT "http://127.0.0.1/$bucket_name/$a_object_name", "Content-Type" => "text/plain", Content => $create_data->(1) );

            if (! ok $res->is_success, "PUT while a storage is down is successful") {
                diag $res->as_string;
            }
            my $clusters = $dbh->selectall_arrayref(<<EOSQL, { Slice => {} }, $bucket_name, $a_object_name );
                SELECT s.cluster_id
                    FROM storage s
                        JOIN entity e ON s.id = e.storage_id
                        JOIN object o ON o.id = e.object_id
                        JOIN bucket b ON b.id = o.bucket_id
                    WHERE b.name = ? AND o.name = ?
EOSQL
            my @match = grep { $_->{cluster_id} == $broken->{cluster_id} } @$clusters;
            ok !@match, "object $bucket_name/$a_object_name does not belong to cluster $broken->{cluster_id}";
        }
    }

    clear_queue();
    note "POST /$bucket_name/$object_name";
    $res = $cb->(
        POST "http://127.0.0.1/$bucket_name/$object_name",
            'X-STF-Replication-Count' => 10
    );
    if (! ok $res->is_success, "POST is successful") {
        diag $res->as_string;
    }

    {
        my $backends = $ENV{ STF_STORAGE_SIZE } ||= 3;
        my $guard = $context->container->new_scope();
        my $worker = STF::Worker::Replicate->new(
            container => $context->container,
            max_works_per_child => 1,
        );
        $worker->work;

        # Check that we have exactly $backends entities
        my $dbh = $context->container->get('DB::Master');
        my ($e_count) = $dbh->selectrow_array( <<EOSQL, undef, $bucket_name, $object_name);
            SELECT count(*) FROM entity e
                JOIN object o ON e.object_id = o.id 
                JOIN bucket b ON o.bucket_id = b.id
                WHERE b.name = ? AND o.name = ?
EOSQL
        is $e_count, $backends, "After changing the replication count, there are exactly $backends entities (got $e_count)";
    }

    for my $make_inactive ( 0, 1, 0 ) {
        if ($make_inactive) {
            note "Making object $bucket_name/$object_name inactive...";
            my $dbh = $context->container->get('DB::Master');
            my ($object_id) = $dbh->selectrow_array( <<EOSQL, undef, $bucket_name, $object_name );
                SELECT o.id FROM object o
                    JOIN bucket b ON o.bucket_id = b.id
                    WHERE b.name = ? AND o.name = ?
EOSQL

            $dbh->do( <<EOSQL, undef, $object_id );
                UPDATE object o SET o.status = 0 WHERE o.id = ?
EOSQL
        }
            
        # re-put an object with new content
        ($content, $content_hash) = $create_data->($chunks);

        note "PUT to /$bucket_name/$object_name, replication count = 3 (write-over)";

        $res = $cb->(
            PUT "http://127.0.0.1/$bucket_name/$object_name",
                "X-Replication-Count" => 3,
                "Content-Type"        => "text/plain",
                "Content"             => $content,
        );
        if (! ok $res->is_success, "object creation (write-over) request was successful") {
            diag $res->as_string;
        }

        foreach my $path ( "/$bucket_name/$object_name", "/$bucket_name//$object_name" ) {
            note "GET $path";
            $res = $cb->(
                GET "http://127.0.0.1/$path"
            );
            if (! ok $res->is_success, "GET is successful") {
                diag $res->as_string;
            } else {
                is md5_hex($res->content), $content_hash, "content matches";
            }
        }
    }

    { 
        note "Moving object...";
        $res = $cb->( PUT "http://127.0.0.1/$bucket_name.2" );
        if (! ok $res->is_success, "Bucket creation should succeed" ) {
            diag $res->as_string;
        }

        note "First, moving to the same bucket, different name";
        my $move_req = HTTP::Request->new( MOVE => "http://127.0.0.1/$bucket_name/$object_name" );
        $move_req->header( 'X-STF-Move-Destination', "/$bucket_name/$object_name.2" );
        $res = $cb->( $move_req );
        if (! ok $res->is_success, "Move successful (1)") {
            diag $res->as_string;
        }

        $res = $cb->( GET "http://127.0.0.1/$bucket_name/$object_name.2" );
        if (! ok $res->is_success, "GET is successful (after rename to $object_name.2") {
            diag $res->as_string;
        } else {
            is md5_hex($res->content), $content_hash, "content matches";
        }

        note "Now moving to a different bucket, differnt name";
        $move_req = HTTP::Request->new( MOVE => "http://127.0.0.1/$bucket_name/$object_name.2" );
        $move_req->header( 'X-STF-Move-Destination', "/$bucket_name.2/$object_name" );
        $res = $cb->( $move_req );
        if (! ok $res->is_success, "Move successful (2)" ) {
            diag $res->as_string;
        }

        $res = $cb->( GET "http://127.0.0.1/$bucket_name.2/$object_name" );
        if (! ok $res->is_success, "GET is successful (after rename to bucket $bucket_name.2") {
            diag $res->as_string;
        } else {
            is md5_hex($res->content), $content_hash, "content matches";
        }

        note "Now moving back to the original location";
        $move_req = HTTP::Request->new( MOVE => "http://127.0.0.1/$bucket_name.2/$object_name" );
        $move_req->header( 'X-STF-Move-Destination', "/$bucket_name/$object_name" );
        $res = $cb->( $move_req );
        if (! ok $res->is_success, "Move successful (3)" ) {
            diag $res->as_string;
        }

        $res = $cb->( GET "http://127.0.0.1/$bucket_name/$object_name" );
        if (! ok $res->is_success, "GET is successful (after rename to bucket $object_name") {
            diag $res->as_string;
        } else {
            is md5_hex($res->content), $content_hash, "content matches";
        }
    }

    note "DELETE /$bucket_name/$object_name";
    $res = $cb->(
        DELETE "http://127.0.0.1/$bucket_name/$object_name"
    );
    if (! ok $res->is_success, "object deletion request was successful") {
        diag $res->as_string;
    }

    {
        my $guard = $context->container->new_scope();
        my $worker = STF::Worker::DeleteObject->new(
            container => $context->container,
            max_works_per_child => 1,
        );
        $worker->work;

        # Check that we have exactly $backends entities
        my $dbh = $context->container->get('DB::Master');
        my ($e_count) = $dbh->selectrow_array( <<EOSQL, undef, $bucket_name, $object_name);
            SELECT count(*) FROM entity e
                JOIN object o ON e.object_id = o.id 
                JOIN bucket b ON o.bucket_id = b.id
                WHERE b.name = ? AND o.name = ?
EOSQL
        is $e_count, 0, "there are exactly 0 entities (should now be deleted)";
    }

    note "GET /$bucket_name/$object_name (After DELETE)";
    $res = $cb->(
        GET "http://127.0.0.1/$bucket_name/$object_name",
    );
    if (! is $res->code, 404, "get after delete is 404" ) {
        diag $res->as_string;
    }

    # if the cluster is not in READ/WRITE mode, then we shouldn't be 
    # writing to that cluster.
    my $verify_cluster_is_not_written_to = sub {
        my ($ro_cluster, $extra_message) = @_;

        my $base = random_string(16);
        $cb->( PUT "http://127.0.0.1/$base" );
        for my $i (1..10) {
            my $res = $cb->(
                PUT "http://127.0.0.1/$base/$i",
                    Content => random_string(512)
            );

            if ( ! ok $res->is_success, "PUT is success ($extra_message)" ) {
                diag $res->as_string;
            }
        }

        my $bucket = $context->container->get('API::Bucket')->lookup_by_name( $base );
        my @objects = $context->container->get('API::Object')->search({
            bucket_id => $bucket->{id},
        });
        my $cluster_api = $context->container->get('API::StorageCluster');
        foreach my $object (@objects) {
            my $cluster = $cluster_api->load_for_object( $object->{id} );
            isnt $cluster->{id}, $ro_cluster->{id}, "writes are not happening to readonly cluster $ro_cluster->{id} ($extra_message)";
        }
    };

    {
        my $cluster_api = $context->container->get('API::StorageCluster');
        my @clusters    = $cluster_api->search({}, { order_by => 'rand()' });
        my $ro_cluster  = $clusters[0];

        $cluster_api->update( $ro_cluster->{id}, {
            mode => STORAGE_CLUSTER_MODE_READ_ONLY,
        } );

        my $guard = Guard::guard( sub {
            $cluster_api->update( $ro_cluster->{id}, {
                mode => STORAGE_CLUSTER_MODE_READ_WRITE,
            } );
        });

        $verify_cluster_is_not_written_to->( $ro_cluster, "cluster disabled directly" );
    }

    { # check the trigger that makes the cluster read-only
        my $cluster_api = $context->container->get('API::StorageCluster');
        my @clusters    = $cluster_api->search({}, { order_by => 'rand()' });
        my $ro_cluster  = $clusters[0];

        my $storage_api = $context->container->get('API::Storage');
        my @storages    = $storage_api->search(
            {
                cluster_id => $ro_cluster->{id},
            },
            {
                order_by   => 'rand()'
            }
        );

        my $ro_storage = $storages[0];
        $storage_api->update( $ro_storage->{id}, {
            mode => STORAGE_MODE_TEMPORARILY_DOWN,
        } );

        my $guard = Guard::guard( sub {
            $storage_api->update( $ro_storage->{id}, {
                mode => STORAGE_MODE_READ_WRITE,
            } );
            $cluster_api->update( $ro_cluster->{id}, {
                mode => STORAGE_CLUSTER_MODE_READ_WRITE,
            } );
        });

        $verify_cluster_is_not_written_to->( $ro_cluster, "cluster disabled via storage" );
    }

    note "DELETE /$bucket_name";
    $res = $cb->(
        DELETE "http://127.0.0.1/$bucket_name"
    );
    if (! is $res->code, 204, "bucket deletion request was 204") {
        diag $res->as_string;
    }

    {
        my $guard = $context->container->new_scope();
        my $worker = STF::Worker::DeleteBucket->new(
            container => $context->container,
            max_works_per_child => 1,
        );
        $worker->work;

        # Check that we have exactly $backends entities
        my $dbh = $context->container->get('DB::Master');
        my ($e_count) = $dbh->selectrow_array( <<EOSQL, undef, $bucket_name);
            SELECT count(*) FROM bucket b WHERE b.name = ? 
EOSQL
        is $e_count, 0, "there are exactly 0 buckets (should now be deleted)";
    }

};

# XXX under very rare circumstances, I've seen calls to cwd() fail during
# testing. I'm just going to override this by setting STF_HOME here

$ENV{ STF_HOME } = Cwd::cwd();

my $app = require "t/dispatcher.psgi";
foreach my $impl ( qw(MockHTTP Server) ) {
    # 10KB, 1MB, 2MB, 4MB, 8MB
    foreach my $chunk ( 10, 1_024, 2 * 1_024, 4 * 1024, 8 * 1024 ) {
        note sprintf "(%s) Running tests on %s", __FILE__, $impl;
        note sprintf "   Using chunk size %d", $chunk;
        clear_queue();
        test_psgi
            app => $app,
            client => sub { $code->($chunk, @_) }
        ;
    }
}

done_testing;