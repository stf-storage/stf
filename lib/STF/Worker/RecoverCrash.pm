# XXX  WARNING WARNING WARNING
# This worker is no longer useful in the new clustered scheme.
#
# In the new clustered scheme, this is how you recover from a crash:
#
# Prerequisite:
#   You have at least 2 clusters, each with at least 3 nodes
#
# 1. Mark the failed storage as DOWN
#    - The cluster will automatically be marked as READ_ONLY
#      (XXX This is not implemented yet)
#    - During this time, your storage cluster is running in degraded
#      mode. Reads can happen, but writes will not go to this cluster.
#      (XXX This is not implemented yet)
#
# 2. Replace, fix, do whatever you need to do with the failed storage
#
# 3. Once the storage is fixed, mark another storage in the same 
#    cluster as DOWN
#    - Don't worry, you still have 1 storage, and it's being served READ ONLY
#
# 4. Use rsync or whatever to copy the data from the storage marked down
#    at step 3 to the fixed storage.
#
# 5. Once the data is back in sync, mark the failed storage as READ ONLY,
#    the rsync source as READ WRITE and keep an eye out on the machine status
#    - The cluster should still be READ ONLY
#    - This extra step is to make sure that the recovered storage isn't
#      corrupted or something
#
# 6. Put all the storages back in READ WRITE mode
#    - The cluster should automatically be marked as READ_WRITE
#      (XXX This is not implemented yet)

package STF::Worker::RecoverCrash;
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

        my ($storage) = $api->search( { mode => STORAGE_MODE_CRASH } );
        if (! $storage) {
            if ( STF_DEBUG ) {
                printf STDERR "[     Crash] Nothing to recover\n";
            }
            return;
        }

        my $storage_id = $storage->{id};
        if ( STF_DEBUG ) {
            printf STDERR "[     Crash] Recovering storage %s\n",
                $storage_id
            ;
        }
        $api->update( $storage_id, { mode => STORAGE_MODE_CRASH_RECOVER_NOW } );
        my $guard = Guard::guard {
            STF::Utils::timeout_call( 2, sub {
                eval {
                    $api->update( $storage_id, { mode => STORAGE_MODE_CRASH } );
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
                    print STDERR "[     Crash] Received signal, stopping recover\n";
                }
                die "Received signal $sig, bailing out";
            };
        };
        local $SIG{INT}  = $sig->("INT");
        local $SIG{QUIT} = $sig->("QUIT");
        local $SIG{TERM} = $sig->("TERM");
        my $processed = $api->move_entities( $storage_id, sub {
            my $now = $api->lookup( $storage_id );
            return $now->{mode} == STORAGE_MODE_CRASH_RECOVER_NOW;
        } );

        $guard->cancel;
        if (STF_DEBUG) {
            printf STDERR "[     Crash] Storage %d, processed %d rows\n",
                $storage_id, $processed;
        }
        $api->update( $storage_id => { mode => STORAGE_MODE_CRASH_RECOVERED } );
    };
    if (my $e = $@) {
        if ($e !~ /Received signal/) {
            Carp::confess("Failed to run recover crash: $e");
        }
    }
}

no Mouse;

1;