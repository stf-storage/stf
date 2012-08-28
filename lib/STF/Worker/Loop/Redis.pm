package STF::Worker::Loop::Redis;
use Mouse;
use Time::HiRes ();
use Redis;
use STF::Log;
use STF::Constants qw(STF_DEBUG);
use feature 'state';

extends 'STF::Worker::Loop';

sub work {
    my ($self, $impl) = @_;

    local $STF::Log::PREFIX = "Loop::Redis" if STF_DEBUG;

    my $queue_name = $self->queue_name;
    my $redis = $impl->get($queue_name);
    my $func = Scalar::Util::blessed($impl);
    $func =~ s/^STF::Worker:://;
    $func =~ s/([a-z])([A-Z])/"$1_$2"/eg;
    $func = lc $func;

    my $decoder = $impl->get('JSON');
    my $loop = 1;
    $SIG{TERM} = $SIG{INT} = $SIG{QUIT} = sub {
        critf("Signal received");
        $loop = 0;
    };
    while ( $loop && $self->should_loop ) {
        my $payload = $redis->lpop($func);
        if ($payload) {
            my $job = $decoder->decode($payload);
            eval {
                $impl->work_once( $job->{args}->[0] );
            }
        }
        $self->incr_processed;
        Time::HiRes::usleep($self->interval);
    }
}

1;
