package STF::AdminWeb::Controller::Config;
use Mouse;
use JSON();
use Time::HiRes ();
use STF::Utils;

extends 'STF::AdminWeb::Controller';

sub list {
    my ($self, $c) = @_;

    my $config_vars = $c->get('API::Config')->search({});
    $c->stash->{config_vars} = $config_vars;
    my %fdat;
    foreach my $pair (@$config_vars) {
        $fdat{ $pair->{varname} } = $pair->{varvalue};
    }
    $self->fillinform( $c, \%fdat );

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
        (map { "stf.worker.$_.processed_jobs" } 
            qw(ContinuousRepair DeleteBucket DeleteObject RepairObject RepairStorage Replicate StorageHealth))
    );
    $c->stash->{states} = $h;
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
