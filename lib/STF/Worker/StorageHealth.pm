package STF::Worker::StorageHealth;
use Mouse;
use STF::Constants qw(:storage STF_DEBUG);
use STF::Utils ();

extends 'STF::Worker::Base';
with 'STF::Trait::WithContainer';

has test_path => (
    is => 'ro',
    default => "/test/test.txt"
);

has '+interval' => (
    default => 5 * 60 * 1_000_000
);

sub work {
    my $self = shift;

    my $storage_api = $self->get('API::Storage');
    my $furl = $self->get('Furl');

    my @storages = $storage_api->search( {
        mode => { in => [ STORAGE_MODE_READ_ONLY, STORAGE_MODE_READ_WRITE ] }
    });
    foreach my $storage ( @storages ) {
        my $uri = $storage->{uri} . $self->test_path;
        my (undef, $code, undef, $hdrs, $body) = $furl->get($uri);

        if ($code eq 200) {
            next;
        }
        printf STDERR <<EOM, $uri, $storage->{id}, $storage->{uri}, $code;
[StorageHealth] CRITICAL! FAILED TO GET '%s'
[StorageHealth]    storage id : %d
[StorageHealth]    storage uri: %s
[StorageHealth]    response   : %d
[StorageHealth] GOING TO BRING DOWN THIS STORAGE!
EOM
        $storage_api->update(
            { id => $storage->{id}, updated_at => $storage->{updated_at} },
            {
                mode => STORAGE_MODE_TEMPORARILY_DOWN,
                updated_at => \'NOW()',
            }
        );
    }
}

no Mouse;

1;