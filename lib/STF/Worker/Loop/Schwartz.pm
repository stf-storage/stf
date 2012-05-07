package STF::Worker::Loop::Schwartz;
use Mouse;
use Scalar::Util ();
use STF::Constants qw(STF_DEBUG);
use STF::Log;
use TheSchwartz;
use Time::HiRes ();

extends 'STF::Worker::Loop';
with 'STF::Trait::WithContainer';

sub create_client {
    my ($self, $impl) = @_;

    local $STF::Log::PREFIX = "Schwartz";
    my $dbh = $self->get('DB::Queue') or
        Carp::confess( "Could not fetch DB::Queue" );
    my $driver = Data::ObjectDriver::Driver::DBI->new( dbh => $dbh );
    my $client = TheSchwartz->new( databases => [ { driver => $driver } ] );

    # XXX Hack! TheSchwartz only allows classnames to be registered to
    # the worker. I hate it. But you can always workaround it by wasting
    # one GV and creating a proxy class name.
    my $ability = Scalar::Util::blessed($impl) . '::Proxy';
    {
        no strict 'refs';
        require TheSchwartz::Worker;
        @{ "${ability}::ISA" } = qw(TheSchwartz::Worker);
        *{ "${ability}::work" } = sub {
            my ($class, $job) = @_;

            my $extra_guard;
            if ( STF_DEBUG ) {
                debugf("---- START %s:%s ----", $ability, $job->arg) if STF_DEBUG;
                $extra_guard = Guard::guard(sub {
                    debugf("---- END %s:%s ----", $ability, $job->arg) if STF_DEBUG;
                } );
            }

            eval {
                $impl->work_once( $job->arg );
            };
            # XXX Retry? Naahhhh
            if ($@) {
                critf("Error from work_once: %s", $@);
            }
            eval { $job->completed };
        };
    }
    $client->can_do( $ability );

    return $client;
}

sub work {
    my ($self, $impl) = @_;

    my $client = $self->create_client($impl);
    while ( $self->should_loop ) {
        if ( $client->work_once ) {
            $self->incr_processed;
        } else {
            if ( (my $interval = $self->interval) > 0 ) {
                Time::HiRes::usleep( $interval );
            }
        }
    }
}

no Mouse;

1;