package STF::Worker::RepairStorage;
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

        # There may be multiple storages, but we only process one at a time,
        # so just pick one

        my ($storage) = $api->search( { mode => STORAGE_MODE_REPAIR } );
        if (! $storage) {
            if ( STF_DEBUG ) {
                printf STDERR "[    Repair] No storage to repair\n";
            }
            return;
        }

        my $storage_id = $storage->{id};
        if ( STF_DEBUG ) {
            printf STDERR "[    Repair] Repairing storage %s\n",
                $storage_id
            ;
        }
        my $ok = $api->update( $storage_id,
            { mode => STORAGE_MODE_REPAIR_NOW, updated_at => \'NOW()' },
            { updated_at => $storage->{updated_at} }
        );
        if (! $ok) {
            if ( STF_DEBUG ) {
                printf STDERR "[    Repair] Could not update storage, bailing out\n";
            }
            return;
        }
        my $guard = Guard::guard {
            STF::Utils::timeout_call( 2, sub {
                eval {
                    $api->update( $storage_id,
                        { mode => STORAGE_MODE_REPAIR, updated_at => \'NOW()' },
                    );
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
                    print STDERR "[     Crash] Received signal, stopping repair\n";
                }
                die "Received signal $sig, bailing out";
            };
        };
        local $SIG{INT}  = $sig->("INT");
        local $SIG{QUIT} = $sig->("QUIT");
        local $SIG{TERM} = $sig->("TERM");

        my $bailout = 0;
        my $limit = 10_000;
        my $object_id = 0;
        my $processed = 0;
        my $queue_api = $self->get('API::Queue');
        my $dbh = $self->get('DB::Master');
        my $sth = $dbh->prepare(<<EOSQL);
            SELECT object_id FROM entity WHERE storage_id = ? AND object_id > ? LIMIT $limit
EOSQL
        my $size = $queue_api->size( 'repair_object' );
        while ( $sth->execute( $storage_id, $object_id ) > 0 ) {
            $sth->bind_columns( \($object_id) );
            while ( $sth->fetchrow_arrayref ) {
                $queue_api->enqueue( repair_object => "NP:$object_id" );
                $processed++;
            }

            # wait here until we have processed the rows that we just
            # inserted into the repair queue
            my $prev = $size;
            $size = $queue_api->size( 'repair_object' );
            while ( $size > $prev ) {
                sleep(60 * ($limit / 1_000));
                $size = $queue_api->size( 'repair_object' );
            }

            # Bail out if the value for mode has changed
            my $now = $api->lookup( $storage_id );
            if ( $now->{mode} != STORAGE_MODE_REPAIR_NOW ) {
                $bailout = 1;
                last;
            }
        }

        $guard->cancel;
        if (STF_DEBUG) {
            printf STDERR "[    Repair] Storage %d, processed %d rows\n",
                $storage_id, $processed;
        }
        if (! $bailout) {
            $api->update( $storage_id => { mode => STORAGE_MODE_REPAIR_DONE } );
        }
    };
    if (my $e = $@) {
        if ($e !~ /Received signal/) {
            Carp::confess("Failed to run repair storage: $e");
        }
    }
}

no Mouse;

1;
