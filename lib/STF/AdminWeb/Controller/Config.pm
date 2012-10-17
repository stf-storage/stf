package STF::AdminWeb::Controller::Config;
use Mouse;
use JSON();
use Time::HiRes ();
use STF::Utils;
use STF::API::Throttler;

extends 'STF::AdminWeb::Controller';

sub worker {
    my ($self, $c) = @_;

    my $worker_name = $c->match->{worker_name};

    # Find where this worker should be running on
    my @drones;
    {
        my $dbh = $c->get('DB::Master');
        my $sth = $dbh->prepare(<<EOSQL);
            SELECT drone_id FROM worker_instances WHERE worker_type = ?
EOSQL
        $sth->execute( $worker_name );
        my $drone;
        $sth->bind_columns(\($drone));
        while ($sth->fetchrow_arrayref) {
            push @drones, $drone;
        }
    }

    # XXX Throttler API sucks. fix it
    # Get the current throttling count
    my $throttler = STF::API::Throttler->new(
        key => "stf.worker.$worker_name.processed_jobs",
        throttle_span => 10,
        container => $c->container,
    );
    my %states = (
        "stf.worker.$worker_name.processed_jobs" => $throttler->current_count(time()),
    );

    my $prefix = sprintf 'stf.worker.%s.%%', $worker_name;
    my $config_vars = $c->get('API::Config')->search({
        varname => [
            { 'LIKE' => $prefix },
            { 'LIKE' => sprintf 'stf.drone.%s.instances', $worker_name }
        ]
    });

    my $stash = $c->stash;
    $stash->{drones} = \@drones;
    $stash->{states} = \%states;
    $stash->{config_vars} = $config_vars;
    $stash->{worker_name} = $worker_name;
    my %fdat;
    foreach my $pair (@$config_vars) {
        $fdat{ $pair->{varname} } = $pair->{varvalue};
    }
    $self->fillinform( $c, \%fdat );
}

sub list {
    my ($self, $c) = @_;

    my $config_vars = $c->get('API::Config')->search({});
    $c->stash->{config_vars} = $config_vars;
    my %fdat;
    foreach my $pair (@$config_vars) {
        $fdat{ $pair->{varname} } = $pair->{varvalue};
    }
    $self->fillinform( $c, \%fdat );
}

sub reload {
    my ($self, $c) = @_;

    my $memd = $c->get('Memcached');
    my $now = time();
    $memd->set_multi(
        (map { [ "stf.drone.$_", $now ] } qw(election reload balance)),
        (map { [ "stf.worker.$_.reload", $now ] } 
            qw(ContinuousRepair DeleteBucket DeleteObject RepairObject RepairStorage Replicate StorageHealth))
    );

    my $response = $c->response;
    $response->code( 200 );
    $response->content_type("application/json");
    $c->finished(1);

    $response->body(JSON::encode_json({ message => "reload flag set properly" }));
}

sub update {
    my ($self, $c) = @_;

    my $p = $c->request->parameters;
    my @params = map { ($_ => $p->get_one($_)) } $p->keys;

    $c->get('API::Config')->set(@params);
    $c->redirect( $c->uri_for("/config/list") );
}


no Mouse;

1;
