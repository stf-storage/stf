package STF::Worker::WithLeaderElection;
use Mouse::Role;
use STF::Log;
use STF::Constants qw(STF_DEBUG);
use Scalar::Util ();

with 'STF::Trait::WithLeaderElection';

around work => sub {
    my ($next, $self) = @_;

    my $guard = $self->elect_leader();
    if (! $guard) {
        if (STF_DEBUG) {
            debugf("Worker %s (%d) timed out/bailed out of leader election",
                Scalar::Util::blessed($self), $$);
        }
        return;
    }

    if (STF_DEBUG) {
        debugf("Worker %s (%d) won the leader election. Going to proceed...",
            Scalar::Util::blessed($self), $$);
    }
    $self->$next();
    undef $guard;
};

no Mouse::Role;

1;
