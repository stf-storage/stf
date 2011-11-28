use strict;
use Test::More;
use HTTP::Request::Common qw(PUT);
use STF::Test;

use_ok "STF::Context";

subtest 'sanity' => sub {
    my $context = STF::Context->bootstrap( config => "t/config.pl" );
    my $api = $context->get( 'API::Object' );
    ok($api);
};

subtest 'object_with_entity_count' => sub {
    my $context = STF::Context->bootstrap( config => "t/config.pl" );
    my $bucket_api = $context->get( 'API::Bucket' );
    my $api = $context->get( 'API::Object' );

    TODO: {
        # XXX need to create objects to really test this
        todo_skip "XXX Need to create objects to test this", 2;

    my @buckets = $bucket_api->search();
    foreach my $bucket (@buckets) {
        my @objects = $api->search_with_entity_info({ bucket_id => $bucket->{id} });
        foreach my $object( @objects ) {
            ok exists $object->{entity_count};
            note "$object->{id} = $object->{entity_count}";
        }
    }

    }
};

done_testing;