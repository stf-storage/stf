package STF::Worker::StorageHealth;
use Mouse;
use Digest::MD5 ();
use STF::Constants qw(:storage STF_DEBUG);
use STF::Utils ();

extends 'STF::Worker::Base';
with 'STF::Trait::WithContainer';

has '+interval' => (
    default => 5 * 60 * 1_000_000
);

sub work_once {
    my $self = shift;

    my $storage_api = $self->get('API::Storage');
    my $furl = $self->get('Furl');

    my @storages = $storage_api->search( {
        mode => { in => [ STORAGE_MODE_READ_ONLY, STORAGE_MODE_READ_WRITE ] }
    });
    foreach my $storage ( @storages ) {
        # Create a health.txt with random content
        my $content = Digest::MD5::md5_hex({}, $$, rand(), time());
        my $uri     = "$storage->{uri}/health.txt";

        # DELETE the object first, jut in case
        eval {
            $furl->delete($uri);
        };

        # now do a successive PUT/HEAD/GET/DELETE
        my @res;
        eval {
            @res = $furl->put($uri, undef, $content);
            if ($res[1] !~ /^20\d/) {
                die "Failed to PUT";
            }
            @res = $furl->head($uri);
            if ($res[1] !~ /^20\d/) {
                die "Failed to HEAD";
            }
            @res = $furl->get($uri);
            if ($res[1] !~ /^20\d/) {
                die "Failed to GET";
            }
            @res = $furl->delete($uri);
            if ($res[1] !~ /^20\d/) {
                die "Failed to DELETE";
            }
        };
        if (my $e = $@) {
            # we got an error...
            printf STDERR <<EOM, $uri, $e, $storage->{id}, $storage->{uri}, $res[1];
[StorageHealth] CRITICAL! FAILED TO GET '%s'
[StorageHealth]    error      : %s
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
}

no Mouse;

1;