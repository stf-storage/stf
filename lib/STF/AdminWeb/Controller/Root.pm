package STF::AdminWeb::Controller::Root;
use Mouse;
use STF::API::Throttler;

extends 'STF::AdminWeb::Controller';

sub index {}
sub state {
    my ($self, $c) = @_;
    # Load the current state of leader election
    # XXX Wrap in ::API ?
    my $dbh = $c->get('DB::Master');
    my $list = $dbh->selectall_arrayref(<<EOSQL, { Slice => {} });
        SELECT * FROM worker_election ORDER BY id ASC
EOSQL
    $c->stash->{election} = $list;

    my $memd = $c->get('Memcached');
    my $h = $memd->get_multi(
        (map { "stf.drone.$_" } qw(election reload balance)),
    );
    my $throttler = STF::API::Throttler->new(
        key => "DUMMY",
        throttle_span => 10,
        container => $c->container,
    );
    $h = {
        %$h,
        %{ $throttler->current_count_multi(
            time(),
            map { "stf.worker.$_.processed_jobs" }
                qw(ContinuousRepair DeleteBucket DeleteObject RepairObject RepairStorage Replicate StorageHealth)
        ) }
    };

    $c->stash->{states} = $h;
}

no Mouse;

1;