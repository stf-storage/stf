package STF::Worker::RepairStorage;
use Mouse;
use Scope::Guard ();
use STF::Constants qw(:storage STF_DEBUG);
use STF::Utils ();
use STF::Log;

extends 'STF::Worker::Base';
with 'STF::Trait::WithContainer';

has '+interval' => (
    default => 5 * 60 * 1_000_000
);

sub work_once {
    my $self = shift;

    my $o_e0 = $0;
    my $guard = Scope::Guard->new(sub {
        $0 = $o_e0;
    });
    local $STF::Log::PREFIX = "Repair(S)" if STF_DEBUG;
    eval {
        my $api = $self->get('API::Storage');

        # There may be multiple storages, but we only process one at a time,
        # so just pick one

        my ($storage) = $api->search( {
            mode => { -in => [ STORAGE_MODE_REPAIR_OBJECT, STORAGE_MODE_REPAIR_ENTITY ] }
        } );
        if (! $storage) {
            infof("No storage to repair");
            return;
        }

        my $o_mode = $storage->{mode};
        my $new_mode = ($o_mode == STORAGE_MODE_REPAIR_OBJECT) ?
            STORAGE_MODE_REPAIR_OBJECT_NOW :
            STORAGE_MODE_REPAIR_ENTITY_NOW
        ;
            

        my $storage_id = $storage->{id};
        infof("Repairing storage %s", $storage_id) if STF_DEBUG;
        my $ok = $api->update( $storage_id,
            { mode => $new_mode, updated_at => \'NOW()' },
            { updated_at => $storage->{updated_at} }
        );
        if (! $ok) {
            warnf("Could not update storage, bailing out");
            return;
        }
        my $guard = Scope::Guard->new(sub {
            STF::Utils::timeout_call( 2, sub {
                local $@;
                eval {
                    $api->update( $storage_id,
                        { mode => $o_mode, updated_at => \'NOW()' },
                    );
                };
            } );
        });

        # Signals terminate the process, but don't allow us to fire the
        # guard object, so we manually fire it up
        my $loop = 1;
        my $sig   = sub {
            my $sig = shift;
            return sub {
                $loop = 0;
                undef $guard;
                croakf("Received signal, stopping repair");
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
            SELECT object_id FROM entity WHERE storage_id = ? ORDER BY object_id ASC LIMIT $limit
EOSQL
        my $size = $queue_api->size( 'repair_object' );
        while ( $loop && $sth->execute( $storage_id ) > 0 ) {
            $sth->bind_columns( \($object_id) );
            while ( $sth->fetchrow_arrayref ) {
                $queue_api->enqueue( repair_object => "NP:$object_id" );
                $processed++;
                $0 = "$o_e0 (object_id: $object_id, $processed)";
            }

            # wait here until we have processed the rows that we just
            # inserted into the repair queue
            my $prev = $size;
            $size = $queue_api->size( 'repair_object' );
            while ( $size > $prev && abs($prev - $size) > $limit * 0.05 ) {
                sleep(60);
                $size = $queue_api->size( 'repair_object' );
            }

            # Bail out if the value for mode has changed
            my $now = $api->lookup( $storage_id );
            if ( $now->{mode} != $new_mode ) {
                $bailout = 1;
                last;
            }
        }

        $guard->dismiss;
        infof("Storage %d, processed %d rows", $storage_id, $processed );
        if (! $bailout) {
            $api->update( $storage_id => { mode => STORAGE_MODE_REPAIR_DONE } );
        }
    };
    if (my $e = $@) {
        if ($e !~ /Received signal/) {
            Carp::confess("Failed to run repair storage: $e");
        } else {
            Carp::confess("Bailing out because of signal; $e" );
        }
    }
}

no Mouse;

1;
