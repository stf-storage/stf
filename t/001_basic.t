use strict;
use Cwd ();
use Digest::MD5 qw(md5_hex);
use Test::More;
use Plack::Test;
use HTTP::Request::Common qw(PUT HEAD GET DELETE POST);
use HTTP::Date;
use STF::Test qw(ts_request clear_queue);

use_ok "STF::Context";
use_ok "STF::Worker::DeleteBucket";
use_ok "STF::Worker::DeleteObject";
use_ok "STF::Worker::Replicate";

# String::URandomとか使っても良いけど面倒くさい
my $random_string = sub {
    my @chars = ('a'..'z');
    join "", map { $chars[ rand @chars ] } 1..($_[0] || 8);
};

my $create_data = sub {
    my $chunks = shift;
    my $content;
    my $md5 = Digest::MD5->new;
    for (1..$chunks) {
        my $piece = $random_string->(1024);
        $md5->add($piece);
        $content .= $piece;
    }
    return ($content, $md5->hexdigest);
};

my $code = sub {
    my ($chunks, $cb) = @_;
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
        ok $object, "found object matchin $bucket_name + $object_name";
    }

    {
        my $guard = $context->container->new_scope();
        my $dbh = $context->container->get('DB::Queue');
        my $worker = STF::Worker::Replicate->new(
            container => $context->container,
            max_works_per_child => 1,
        );
        $worker->work;

        # Check that we have exactly 2 entities
        my $dbh = $context->container->get('DB::Master');
        my ($e_count) = $dbh->selectrow_array( <<EOSQL, undef, $bucket_name, $object_name);
            SELECT count(*) FROM entity e
                JOIN object o ON e.object_id = o.id 
                JOIN bucket b ON o.bucket_id = b.id
                WHERE b.name = ? AND o.name = ?
EOSQL
        is $e_count, 2, "After replication, there are exactly 2 entities created by the worker";
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