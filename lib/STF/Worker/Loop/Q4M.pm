package STF::Worker::Loop::Q4M;
use strict;
use parent qw(
    STF::Worker::Loop
    STF::Trait::WithDBI
);
use POSIX qw(:signal_h);
use Guard ();
use Scalar::Util ();
use Time::HiRes ();
use STF::Constants qw(STF_DEBUG);
use Class::Accessor::Lite
    rw => [ qw(interval) ]
;

sub queue_table {
    my $self = shift;
    my $table = (split /::/, Scalar::Util::blessed $self)[-1];
    $table =~ s/([a-z0-9])([A-Z])/$1_$2/g;
    return sprintf 'queue_%s', lc $table;
}

sub work {
    my $self = shift;

    my $guard = $self->container->new_scope();

    my $table = $self->queue_table;
    my $dbh = $self->get('DB::Queue') or
        Carp::confess( "Could not fetch DB::Queue" );

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
    while ( $self->should_loop ) {
        $sth = $dbh->prepare(<<EOSQL);
            SELECT args FROM $table WHERE queue_wait('$table', 60)
EOSQL
        my $rv = $sth->execute();
        if ($rv == 0) { # nothing found
            $sth->finish;
            next;
        }

        $sth->bind_columns( \$object_id );
        while ( $self->should_loop && $sth->fetchrow_arrayref ) {
            my $extra_guard;
            if (STF_DEBUG) {
                my ($row_id) = $dbh->selectrow_array( "SELECT queue_rowid()" );
                printf STDERR "[ Loop::Q4M] ---- START %s:%s ----\n", $table, $row_id;
                printf STDERR "[ Loop::Q4M] Got new item from %s (%s)\n",
                    $table,
                    $object_id
                ;
                $extra_guard = Guard::guard(sub {
                    printf STDERR "[ Loop::Q4M] ---- END %s:%s ----\n", $table, $row_id;
                } );
            }
            $self->incr_processed();
            my $sig_guard = Guard::guard(\&$setsig);

            # XXX Disable signal handling during work_once
            POSIX::sigaction( SIGINT,  $default );
            POSIX::sigaction( SIGQUIT, $default );
            POSIX::sigaction( SIGTERM, $default );

            my $perloop_scope = $self->container->new_scope(1);
            $self->work_once( $object_id );
            if ( (my $interval = $self->interval) > 0 ) {
                Time::HiRes::usleep( $interval );
            }
        }
    }
    eval { $dbh->do("SELECT queue_end()") };

    if ( STF_DEBUG ) {
        print STDERR "Process $$ exiting...\n";
    }
}

1;