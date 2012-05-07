use strict;
use Test::More;
use HTTP::Request::Common;
use STF::Test qw(random_string clear_queue);
use STF::Constants qw(
    STORAGE_CLUSTER_MODE_READ_WRITE
    STORAGE_CLUSTER_MODE_READ_ONLY
);
use Plack::Test;
use Guard ();

my $code = sub {
    my $cb = shift;

    my $context = STF::Context->bootstrap( config => "t/config.pl" ) ;
    my $container   = $context->container;
    my $bucket_name = random_string();
    my $object_name = random_string(32);
    my $content     = random_string(1024);
    { # create a bucket
        my $res = $cb->(
            PUT "http://127.0.0.1/$bucket_name"
        );
        if (! ok $res->is_success, "bucket creation request was successful") {
            diag $res->as_string;
        }
    }

    my $bucket = $container->get('API::Bucket')->lookup_by_name( $bucket_name );

    { # create an object
        my $res = $cb->(
            PUT "http://127.0.0.1/$bucket_name/$object_name",
            Content => $content,
        );
        if (! ok $res->is_success, "object creation request was successful") {
            diag $res->as_string;
        }

        # Make sure we can get it
        $res = $cb->(
            GET "http://127.0.0.1/$bucket_name/$object_name"
        );
        if (! ok $res->is_success, "object get was successful") {
            diag $res->as_string;
        }

        is $res->content_length, 1024;
    }

    my $guard;
    my $bad_cluster;
    my $good_cluster;

    {
        my $object_api = $container->get('API::Object');
        my $object_id  = $object_api->find_object_id({ bucket_id => $bucket->{id}, object_name => $object_name });
        my $object     = $object_api->lookup($object_id);

        ok $object, "got object $object_id";

        # find the cluster that this object belongs to
        my $cluster_api = $container->get('API::StorageCluster');
        $bad_cluster    = $cluster_api->load_for_object( $object_id );
        ok $bad_cluster, "got cluster at $bad_cluster->{id}";

        # make the cluster readonly
        $cluster_api->update( $bad_cluster->{id}, { mode => STORAGE_CLUSTER_MODE_READ_ONLY } );
        $guard = Guard::guard(sub {
            $cluster_api->update( $bad_cluster->{id}, { mode => STORAGE_CLUSTER_MODE_READ_WRITE } );
        });
    }

    { # now check that we can read from it
        my $res = $cb->(
            GET "http://127.0.0.1/$bucket_name/$object_name"
        );

        if (! ok $res->is_success, "object get after cluster readonly") {
            diag $res->as_string;
        }
        is $res->content, $content;
    }

    my $content2 = random_string(512);
    { # put again, see what happens
        my $res = $cb->(
            PUT "http://127.0.0.1/$bucket_name/$object_name",
            Content => $content2,
        );
        if (! ok $res->is_success, "object creation request was successful") {
            diag $res->as_string;
        }

        # Make sure we can get it
        $res = $cb->(
            GET "http://127.0.0.1/$bucket_name/$object_name"
        );
        if (! ok $res->is_success, "object get was successful") {
            diag $res->as_string;
        }

        is $res->content, $content2;
    }

    # object should have been deleted + created, so 
    {
        my $object_api = $container->get('API::Object');
        my $object_id  = $object_api->find_object_id({ bucket_id => $bucket->{id}, object_name => $object_name });
        my $object     = $object_api->lookup($object_id);

        ok $object, "got object $object_id";

        # find the cluster that this object belongs to
        my $cluster_api  = $container->get('API::StorageCluster');
        my $good_cluster = $cluster_api->load_for_object( $object_id );
        ok $good_cluster, "got cluster at $good_cluster->{id}";

        isnt $good_cluster->{id}, $bad_cluster->{id};
    }
};

$ENV{ STF_HOME } = Cwd::cwd();

my $app = require "t/dispatcher.psgi";
foreach my $impl ( qw(MockHTTP Server) ) {
    note sprintf "(%s) Running tests on %s", __FILE__, $impl;
    clear_queue();
    test_psgi
        app => $app,
        client => $code,
    ;
}

done_testing;

