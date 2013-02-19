package STF::AdminWeb::Controller::Cluster;
use Mojo::Base 'STF::AdminWeb::Controller';
use JSON ();

sub load_cluster {
    my ($self) = @_;
    my $cluster_id = $self->match->captures->{object_id};
    my $cluster = $self->get('API::StorageCluster')->lookup( $cluster_id );
    if (! $cluster) {
        return;
    }
    $self->stash(cluster => $cluster);
    return $cluster;
}

sub view {
    my ($self) = @_;
    if (! $self->load_cluster() ) {
        $self->render_not_found();
        return;
    }
    my $cluster = $self->stash->{cluster};
    my $storage_api = $self->get('API::Storage');
    my @storages = $storage_api->search({ cluster_id => $cluster->{id} });
    $cluster->{storages} = \@storages;
}

sub add {}
sub add_post {
    my ($self) = @_;

    my $params = $self->req->params->to_hash;
    my $result = $self->validate( cluster_add => $params );
    if ($result->success) {
        my $valids = $result->valid;
        $self->get('API::StorageCluster')->create( $valids );
        $self->redirect_to( $self->url_for('/cluster', {done => 1}) );
    } else {
        $self->stash(template => 'cluster/add');
    }
}

sub list {
    my ($self) = @_;

    my $limit = $self->req->param('limit') || 100;
    my $pager = $self->pager( $limit );
    my @clusters = $self->get( 'API::StorageCluster' )->search(
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

    my $storage_api = $self->get('API::Storage');
    foreach my $cluster ( @clusters ) {
        my @storages = $storage_api->search({ cluster_id => $cluster->{id} });
        $cluster->{storages} = \@storages;
    }

    my $stash = $self->stash;
    $stash->{clusters} = \@clusters;
    $stash->{pager} = $pager;
}

sub storage_change {
    my ($self) = @_;

    my $cluster_id = $self->req->param('cluster_id');
    my $storage_api = $self->get('API::Storage');
    foreach my $storage_id ( $self->req->parameters->get_all('storage_id[]') ) {
        $storage_api->update( $storage_id, {
            cluster_id => $cluster_id
        } );
    }

    $self->render_json({});
}

sub storage_unclustered {
    my ($self) = @_;
    my @storages = $self->get('API::Storage')->search({ cluster_id => undef });
    $self->stash(cluster_id => $self->req->param('id'));
    $self->stash(storages => \@storages);
}

sub edit {
    my ($self) = @_;

    my $cluster = $self->load_cluster();
    if (! $cluster) {
        $self->render_not_found;
        return;
    }
    $self->fillinform( $cluster );
}

sub edit_post {
    my ($self) = @_;
    my $cluster = $self->load_cluster();
    if (! $cluster) {
        $self->render_not_found;
        return;
    }

    my $params = $self->req->params->to_hash;
    $params->{id} = $cluster->{id};
    my $result = $self->validate( cluster_edit => $params );
    if ($result->success) {
        my $valids = $result->valid;
        my %meta;
        delete $valids->{id};
        my $api = $self->get('API::StorageCluster');
        $api->update( $cluster->{id} => $valids );

        $self->redirect_to( $self->url_for( "/cluster/list", { done => 1 } ) );
    } else {
        $self->stash(template => 'cluster/edit');
        $self->fillinform( $params );
    }
}

sub delete_post {
    my ($self) = @_;

    my $cluster = $self->load_cluster();
    if (! $cluster) {
        $self->render_not_found;
        return;
    }
    my $params = { id => $cluster->{id} };
    my $result = $self->validate( cluster_delete => $params );
    if ( $result->success ) {
        $self->get('API::StorageCluster')->delete( $cluster->{id} );
        $self->redirect_to( $self->url_for( '/cluster/list', {done => 1}) );
    } else {
        $self->stash(template => 'cluster/edit');
        $self->fillinform( $params );
    }
}

1;