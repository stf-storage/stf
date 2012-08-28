package STF::Worker::Loop::Resque;
use Mouse;
use Resque;
use STF::Log;
use STF::Constants qw(STF_DEBUG);
use feature 'state';

extends 'STF::Worker::Loop';

sub work {
    my ($self, $impl) = @_;

    local $STF::Log::PREFIX = "Loop::Resque" if STF_DEBUG;

    my $resque = $impl->get($self->queue_name);
    state $w = $resque->worker;

    # Add a proxy worker to call the impl
    my $impl_class  = Scalar::Util::blessed($impl);
    my $proxy_class = "${impl_class}::Proxy";

    # Resque::Job does a $job->class->require, and since we
    # haven't declared this class in a file, it SILENTLY
    # fails. grrr. so lie to Perl that this class has already
    # been loaded.
    my $defined = $INC{ join( "/", split qr/::/, $proxy_class ) . ".pm" }++;
    if (! $defined) {
        no strict 'refs';
        no warnings 'redefine';
        *{"${proxy_class}::perform"} = sub {
            my $job = shift;
            $impl->work_once($job->args->[0]);
        };

        if ($impl_class !~ /^STF::Worker::(.*)$/) {
            die "$impl_class not supported. Send in patches!";
        }
        my $func = lcfirst $1;
        $func =~ s/([a-z])([A-Z])/join "_", $1, lc $2/eg;
        if (STF_DEBUG) {
            debugf("Worker listening to function %s", $func);
        }
        $w->add_queue( $func );
    }

    my $loop = 1;
    $SIG{TERM} = $SIG{INT} = $SIG{QUIT} = sub {
        critf("Signal received");
        $loop = 0;
    };
    $w->cant_fork(1);
    $w->startup;
    while ( $loop && ! $w->shutdown && $self->max_works_per_child > $w->processed ) {
        if ( !$w->paused && ( my $job = $w->reserve ) ) {
            $w->work_tick($job);
        }
        elsif( $w->interval ) {
            my $status = $w->paused ? "Paused" : 'Waiting for ' . join( ', ', @{$w->queues} );
            $w->procline( $status );
            $w->log( $status );
            sleep( $w->interval );
        }
    }
    $w->unregister_worker;
}

1;
