package STF::AdminWeb::Controller::Config;
use Mojo::Base 'STF::AdminWeb::Controller';
use STF::API::Throttler;

sub notification {
    my ($self) = @_;

    my @rules = $self->get('API::NotificationRule')->search();
    $self->stash(rules => \@rules);
}

sub notification_rule_add {
    my ($self) = @_;

    my $params = $self->req->params->to_hash;
    my $result = $self->validate("notification_rule_add", $params);
    if (! $result->success) {
        $self->notification(); # load stuff
        $self->stash(template => 'config/notification');
        $self->fillinform( $params );
        return;
    }

    $self->get('API::NotificationRule')->create($params);
    $self->redirect_to( $self->url_for("/config/notification") );
}

sub notification_rule_toggle {
    my ($self) = @_;

    my $id = $self->req->param('id');
    my $rule_api = $self->get('API::NotificationRule');
    my $rule = $rule_api->lookup($id);
    $rule_api->update($id, {
        status => $rule->{status} ? 0 : 1,
    });

    $self->render_json({ message => "toggled rule" });
}

sub notification_rule_delete {
    my ($self, $c) = @_;

    my $id = $self->req->param('id');
    $self->get('API::NotificationRule')->delete($id);

    $self->render_json({ message => "deleted rule" });
}

sub worker {
    my ($self, $c) = @_;

    my $worker_name = $self->match->captures->{worker_name};

    # Find where this worker should be running on
    my @drones;
    {
        my $dbh = $self->get('DB::Master');
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
        container => $self->app->context->container,
    );
    my %states = (
        "stf.worker.$worker_name.processed_jobs" => $throttler->current_count(time()),
    );

    my $prefix = sprintf 'stf.worker.%s.%%', $worker_name;
    my $config_vars = $self->get('API::Config')->search({
        varname => [
            { 'LIKE' => $prefix },
            { 'LIKE' => sprintf 'stf.drone.%s.instances', $worker_name }
        ]
    });

    $self->stash(
        drones => \@drones,
        states => \%states,
        config_vars => $config_vars,
        worker_name => $worker_name,
    );
    my %fdat;
    foreach my $pair (@$config_vars) {
        $fdat{ $pair->{varname} } = $pair->{varvalue};
    }
    $self->fillinform( \%fdat );
}

sub worker_list {
    my ($self) = @_;

    my $config_vars = $self->get('API::Config')->search({});
    $self->stash(config_vars => $config_vars);
    my %fdat;
    foreach my $pair (@$config_vars) {
        $fdat{ $pair->{varname} } = $pair->{varvalue};
    }
    $self->fillinform( \%fdat );
}

sub reload {
    my ($self) = @_;

    my $memd = $self->get('Memcached');
    my $now = time();
    $memd->set_multi(
        (map { [ "stf.drone.$_", $now ] } qw(election reload balance)),
        (map { [ "stf.worker.$_.reload", $now ] } 
            qw(ContinuousRepair DeleteBucket DeleteObject RepairObject RepairStorage Replicate StorageHealth))
    );

    $self->render_json({ message => "reload flag set properly" });
}

sub update {
    my ($self, $c) = @_;

    my $p = $self->req->params;
    my @params = map { ($_ => scalar $p->param($_)) } $p->param;

    $self->get('API::Config')->set(@params);
    $self->redirect_to( $self->url_for("/config/list") );
}

1;
