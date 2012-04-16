package STF::AdminWeb::Controller::Cluster;
use Mouse;
use JSON ();
extends 'STF::AdminWeb::Controller';

sub load_cluster {
    my ($self, $c) = @_;
    my $cluster_id = $c->match->{object_id};
    my $cluster = $c->get('API::StorageCluster')->lookup( $cluster_id );
    if (! $cluster) {
        $c->res->status(404);
        $c->abort;
    }
    $c->stash->{cluster} = $cluster;
    return $cluster;
}

sub add {}
sub add_post {
    my ($self, $c) = @_;

    my $params = $c->request->parameters->as_hashref;
    my $result = $self->validate( $c, cluster_add => $params );
    if ($result->success) {
        my $valids = $result->valid;
        $c->get('API::StorageCluster')->create( $valids );
        $c->redirect( $c->uri_for('/cluster', {done => 1}) );
    } else {
        $c->stash->{template} = 'cluster/add';
    }
}

sub list {
    my ($self, $c) = @_;

    my $limit = $c->request->param('limit') || 100;
    my $pager = $c->pager( $limit );
    my @clusters = $c->get( 'API::StorageCluster' )->search(
        {},
        {
            limit    => $pager->entries_per_page + 1,
            offset   => $pager->skipped,
            order_by => { 'id' => 'DESC' },
        }
    );
    if ( @clusters > $limit ) {
        $pager->total_entries( $limit * $pager->current_page + 1 );
        pop @clusters;
    }

    my $storage_api = $c->get('API::Storage');
    foreach my $cluster ( @clusters ) {
        my @storages = $storage_api->search({ cluster_id => $cluster->{id} });
        $cluster->{storages} = \@storages;
    }

    my $stash = $c->stash;
    $stash->{clusters} = \@clusters;
    $stash->{pager} = $pager;
}

sub storage_change {
    my ($self, $c) = @_;

    my $cluster_id = $c->request->param('cluster_id');
    my $storage_api = $c->get('API::Storage');
    foreach my $storage_id ( $c->request->parameters->get_all('storage_id[]') ) {
        $storage_api->update( $storage_id, {
            cluster_id => $cluster_id
        } );
    }

    my $response = $c->response;
    $response->code(200);
    $response->content_type('application/json');
    $response->body( JSON::encode_json( {} ) );

    $c->finished(1);
}

sub storage_unclustered {
    my ($self, $c) = @_;
    my @storages = $c->get('API::Storage')->search({ cluster_id => undef });
    $c->stash->{cluster_id} = $c->request->param('id');
    $c->stash->{storages} = \@storages;
}

sub edit {
    my ($self, $c) = @_;

    my $cluster = $self->load_cluster($c);
    $self->fillinform( $c, $cluster );
}

sub edit_post {
    my ($self, $c) = @_;
    my $cluster = $self->load_cluster($c);

    my $params = $c->request->parameters->as_hashref;
    $params->{id} = $cluster->{id};
    my $result = $self->validate( $c, cluster_edit => $params );
    if ($result->success) {
        my $valids = $result->valid;
        my %meta;
        delete $valids->{id};
        my $api = $c->get('API::StorageCluster');
        $api->update( $cluster->{id} => $valids );

        $c->redirect( $c->uri_for( "/cluster/list", { done => 1 } ) );
    } else {
        $c->stash->{template} = 'cluster/edit';
        $self->fillinform( $c, $params );
    }
}

sub delete_post {
    my ($self, $c) = @_;

    my $cluster = $self->load_cluster($c);
    my $params = { id => $cluster->{id} };
    my $result = $self->validate( $c, cluster_delete => $params );
    if ( $result->success ) {
        $c->get('API::StorageCluster')->delete( $cluster->{id} );
        $c->redirect( $c->uri_for( '/cluster/list', {done => 1}) );
    } else {
        $c->stash->{template} = 'cluster/edit';
        $self->fillinform( $c, $params );
    }
}

no Mouse;

1;