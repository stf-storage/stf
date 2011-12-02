package STF::Worker::Loop::Schwartz;
use strict;
use parent qw(
    STF::Worker::Loop
    STF::Trait::WithContainer
);
use Scalar::Util ();
use TheSchwartz;
use Time::HiRes ();
use Class::Accessor::Lite
    rw => [ qw(interval) ]
;

sub create_client {
    my ($self, $impl) = @_;
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
            eval {
                $impl->work( $job->arg );
            };
            # XXX Retry? Naahhhh
            if ($@) {
                print STDERR $@;
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
            if ( (my $interval = $self->interval) > 0 ) {
                Time::HiRes::usleep( $interval );
            }
        }
    }
}

1;