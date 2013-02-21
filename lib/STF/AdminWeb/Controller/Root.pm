package STF::AdminWeb::Controller::Root;
use Mojo::Base 'STF::AdminWeb::Controller';

sub index {}

sub setlang {
    my ($self) = @_;

    if (my $lang = $self->req->param('lang')) {
        my $localizer = $self->get('Localizer')->set_languages( $lang );
        $self->sessions->set(lang => $lang);
    } 

    $self->redirect_to("/");
}

sub state {
    my ($self) = @_;
    # Load the current state of leader election
    # XXX Wrap in ::API ?
    my $dbh = $self->get('DB::Master');
    my $list = $dbh->selectall_arrayref(<<EOSQL, { Slice => {} });
        SELECT * FROM worker_election ORDER BY id ASC
EOSQL
    $self->stash(election => $list);

    my $memd = $self->get('Memcached');
    my $h = $memd->get_multi(
        (map { "stf.drone.$_" } qw(election reload balance)),
    );
    my $throttler = STF::API::Throttler->new(
        key => "DUMMY",
        throttle_span => 10,
        container => $self->context->container,
    );
    $h = {
        %$h,
        %{ $throttler->current_count_multi(
            time(),
            map { "stf.worker.$_.processed_jobs" }
                qw(ContinuousRepair DeleteBucket DeleteObject RepairObject RepairStorage Replicate StorageHealth)
        ) }
    };

    $self->stash(states => $h);
}

1;