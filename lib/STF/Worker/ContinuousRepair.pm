# ContinuousRepair
#   * only run if there are no other repairs going on
#   * only run if the repair queue isn't big

package STF::Worker::ContinuousRepair;
use Mouse;
use Scope::Guard ();
use STF::Constants qw(:storage STF_DEBUG);
use STF::Utils ();
use STF::Log;

extends 'STF::Worker::Base';
with 'STF::Trait::WithContainer';

has '+interval' => (
    default => 86_400 * 1_000_000
);

sub work_once {
    my $self = shift;

    my $o_e0 = $0;
    my $guard = Scope::Guard->new(sub {
        $0 = $o_e0;
    });
    local $STF::Log::PREFIX = "Repair(CS)" if STF_DEBUG;
    eval {
        # Signals terminate the process, but don't allow us to fire the
        # guard object, so we manually fire it up
        my $loop = 1;
        my $sig   = sub {
            my $sig = shift;
            return sub {
                if (STF_DEBUG) {
                    debugf("Received signal %s", $sig);
                }
                $loop = 0;
                croakf("Received signal %s", $sig);
            };
        };
        local $SIG{INT}  = $sig->("INT");
        local $SIG{QUIT} = $sig->("QUIT");
        local $SIG{TERM} = $sig->("TERM");

        my $bailout = 0;
        my $object_id = 0;
        my $processed = 0;
        my $limit;
        my $queue_api = $self->get('API::Queue');
        my $storage_api = $self->get('API::Storage');
        my $dbh = $self->get('DB::Master');

        # Approximate the number of objects in this system by checking 
        # getting the difference between max/min object_ids
        my ($objcount_guess) = $dbh->selectrow_array(<<EOSQL);
            SELECT (max(id) - min(id) / 1000000000) FROM object
EOSQL
        $objcount_guess ||= 0;
        $objcount_guess = int($objcount_guess);
        if ($objcount_guess <= 0) {
            $limit = 2000;
        } elsif ($objcount_guess > 10_000_000) {
            $limit = 10_000;
        } else {
            $limit = int($objcount_guess / 1_000);
        }

        my $timeout = 0;

        while ( $loop ) {
            my $now = time();
            if ($timeout > $now) {
                select(undef, undef, undef, rand(5));
                next;
            }

            # Only add to queue if there are no more elements to process
            # (i.e. this has the lowest priority)
            my $size = $queue_api->size( 'repair_object' );
            if ( $size > 0 ) {
                $timeout = $now + 60;
                next;
            }

            # Halt this process for a while if there are pending
            # repairs. 
            my @storages = $storage_api->search( {
                mode => { IN => [ 
                    STORAGE_MODE_REPAIR,
                    STORAGE_MODE_REPAIR_NOW,
                ] }
            } );
            if (@storages > 0)  {
                $timeout = $now + 300; # check every 5 minutes
                next;
            }

            my $offset = int rand $limit;
            my $sth = $dbh->prepare(<<EOSQL);
                SELECT id FROM object WHERE id > ? ORDER BY id ASC LIMIT 100 OFFSET $offset 
EOSQL
            if ($sth->execute( $object_id ) <= 0 ) {
                $loop = 0;
                next;
            }

            $sth->bind_columns( \($object_id) );
            while ( $loop && $sth->fetchrow_arrayref ) {
                $queue_api->enqueue( repair_object => "NP:$object_id" );
                $processed++;
                $0 = "$o_e0 (object_id: $object_id, $processed)";
                select(undef, undef, undef, rand 1);
            }
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