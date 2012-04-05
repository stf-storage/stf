package STF::Worker::RetireStorage;
use Mouse;
use STF::Constants qw(:storage STF_DEBUG);
use STF::Utils ();

extends 'STF::Worker::Base';
with 'STF::Trait::WithContainer';

has '+interval' => (
    default => 5 * 60 * 1_000_000
);

sub work_once {
    my $self = shift;

    eval {
        my $api = $self->get('API::Storage');

        my ($storage) = $api->search({ mode => STORAGE_MODE_RETIRE });
        if (! $storage) {
            if ( STF_DEBUG ) {
                printf STDERR "[     Retire] Nothing to retire\n";
            }
            return;
        }

        my $storage_id = $storage->{id};
        if ( STF_DEBUG ) {
            printf STDERR "[     Retire] Recovering storage %s\n",
                $storage_id
            ;
        }
        $api->update( $storage_id, { mode => STORAGE_MODE_MIGRATE_NOW } );
        my $guard = Guard::guard {
            STF::Utils::timeout_call( 2, sub {
                eval {
                    $api->update( $storage_id, { mode => STORAGE_MODE_RETIRE } );
                };
            } );
        };

        # Signals terminate the process, but don't allow us to fire the
        # guard object, so we manually fire it up
        my $sig   = sub {
            my $sig = shift;
            return sub {
                undef $guard;
                if ( STF_DEBUG ) {
                    print STDERR "[     Retire] Received signal, stopping recover\n";
                }
                die "Received signal $sig, bailing out";
            };
        };
        local $SIG{INT}  = $sig->("INT");
        local $SIG{QUIT} = $sig->("QUIT");
        local $SIG{TERM} = $sig->("TERM");

        my $processed = $api->move_entities( $storage_id, sub {
            my $now = $api->lookup( $storage_id );
            return $now->{mode} == STORAGE_MODE_MIGRATE_NOW;
        } );

        $guard->cancel;
        if (STF_DEBUG) {
            printf STDERR "[     Retire] Storage %d, processed %d rows\n",
                $storage_id, $processed;
        }
        $api->update( $storage_id => { mode => STORAGE_MODE_RETIRE } );
    };
    if (my $e = $@) {
        if ($e !~ /Received signal/) {
            Carp::confess("Failed to run recover crash: $e");
        }
    }
}

no Mouse;

1;