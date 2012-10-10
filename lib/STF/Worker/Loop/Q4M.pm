package STF::Worker::Loop::Q4M;
use Mouse;
use POSIX qw(:signal_h);
use Scalar::Util ();
use Time::HiRes ();
use Scope::Guard ();
use STF::Constants qw(STF_DEBUG);
use STF::Log;

extends 'STF::Worker::Loop';
with 'STF::Trait::WithDBI';

sub queue_table {
    my ($self, $impl) = @_;

    if ( my $code = $impl->can('queue_table') ) {
        return $code->($impl);
    }

    my $table = (split /::/, Scalar::Util::blessed $impl)[-1];
    $table =~ s/([a-z0-9])([A-Z])/$1_$2/g;
    return sprintf 'queue_%s', lc $table;
}

sub queue_waitcond {
    my ($self, $impl) = @_;

    if ( my $code = $impl->can('queue_waitcond') ) {
        return $code->($impl);
    }

    $self->queue_table( $impl );
}

sub work {
    my ($self, $impl) = @_;

    local $STF::Log::PREFIX = "Loop::Q4M" if STF_DEBUG;
    my $guard = $self->container->new_scope();

    my $table = $self->queue_table( $impl );
    my $waitcond = $self->queue_waitcond( $impl );
    my $queue_name = $self->queue_name;
    my $dbh = $self->get($queue_name) or
        Carp::confess( "Could not fetch $queue_name" );

    my $loop = 1;
    my $object_id;

    my $sigset = POSIX::SigSet->new( SIGINT, SIGQUIT, SIGTERM );
    my $sth;
    my $cancel_q4m = POSIX::SigAction->new(sub {
        if ( $loop ) {
            eval { $sth->cancel };
            eval { $dbh->disconnect };
            $loop = 0;
        }
    }, $sigset, &POSIX::SA_NOCLDSTOP);
    my $setsig = sub {
        # XXX use SigSet to properly interrupt the process
        POSIX::sigaction( SIGINT,  $cancel_q4m );
        POSIX::sigaction( SIGQUIT, $cancel_q4m );
        POSIX::sigaction( SIGTERM, $cancel_q4m );
    };

    $setsig->();

    my $default = POSIX::SigAction->new('DEFAULT');
    $sth = $dbh->prepare(<<EOSQL);
        SELECT args FROM $table WHERE queue_wait('$waitcond', 60)
EOSQL
    while ( $self->should_loop ) {
        $self->update_now();
        $self->check_state();
        $self->reload();

        $self->incr_processed();
        my $rv = $sth->execute();
        $sth->bind_columns( \$object_id );
        while ( $sth->fetchrow_arrayref ) {
            my $extra_guard;
            if (STF_DEBUG) {
                my ($row_id) = $dbh->selectrow_array( "SELECT queue_rowid()" );
                if (STF_DEBUG) {
                    debugf("---- START %s:%s ----", $table, $row_id);
                    debugf("Got new item from %s (%s)", $table, $object_id);
                }
                $extra_guard = Scope::Guard->new(sub {
                    debugf("---- END %s:%s ----", $table, $row_id) if STF_DEBUG;
                } );
            }

            my $sig_guard = Scope::Guard->new(\&$setsig);

            # XXX Disable signal handling during work_once
            POSIX::sigaction( SIGINT,  $default );
            POSIX::sigaction( SIGQUIT, $default );
            POSIX::sigaction( SIGTERM, $default );

            my $guard = $impl->container->new_scope;
            eval { $impl->work_once( $object_id ) };
            warn $@ if $@;
            if ( (my $interval = $self->interval) > 0 ) {
                Time::HiRes::usleep( $interval );
            }

            $self->throttle();
        }
        eval { $dbh->do("SELECT queue_end()") };
    }

    infof("Process %d exiting... (%s)", $$, $impl);
}

no Mouse;

1;