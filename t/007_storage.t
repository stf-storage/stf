use strict;
use Test::More;
use Test::Fatal;
use Scope::Guard ();

BEGIN {
    use_ok "STF::Context";
    use_ok "STF::Constants", "STF_ENABLE_STORAGE_META";
}

subtest crud => sub {
    my $context = STF::Context->bootstrap();
    my $api = $context->get('API::Storage');
    ok $api;

    my $dbh = $api->get('DB::Master');
    my ($storage_id) = $dbh->selectrow_array( <<EOM, undef );
        SELECT max(id) + 1 FROM storage
EOM

    my $guard = Scope::Guard->new(sub {
        $api->delete( $storage_id );
    });

    is exception {
        $api->create( {
            id => $storage_id,
            uri => "http://0.0.0.0/",
            created_at => time(),
        } );
    }, undef, "no exception";

    is exception {
        my $storage = $api->lookup( $storage_id );
        ok $storage, "got storage for $storage_id";
        if ( STF_ENABLE_STORAGE_META ) {
            ok $storage->{meta}, "meta exists";
        }
    }, undef, "no exception";

    if ( STF_ENABLE_STORAGE_META ) {
        is exception {
            $api->update_meta( $storage_id, {
                notes => "This is a test",
            } );
            my $storage = $api->lookup( $storage_id );
            ok $storage, "got storage for $storage_id";
            if ( STF_ENABLE_STORAGE_META ) {
                is $storage->{meta}->{notes}, "This is a test";
            }
        }, undef, "no exception";
    }
};

done_testing;
