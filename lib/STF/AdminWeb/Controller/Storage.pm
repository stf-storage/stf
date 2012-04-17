package STF::AdminWeb::Controller::Storage;
use Mouse;
use STF::Utils;
use STF::Constants qw(
    STF_ENABLE_STORAGE_META
);

extends 'STF::AdminWeb::Controller';

sub load_storage {
    my ($self, $c) = @_;
    my $storage_id = $c->match->{object_id};
    my $storage = $c->get('API::Storage')->lookup( $storage_id );
    if (! $storage) {
        $c->response->status(404);
        $c->abort;
    }
    $c->stash->{storage} = $storage;
    return $storage;
}

sub list {
    my ($self, $c) = @_;
    my $limit = $c->request->param('limit') || 100;
    my $pager = $c->pager( $limit );
    my @storages = $c->get( 'API::Storage' )->search(
        {},
        {
            limit    => $pager->entries_per_page + 1,
            offset   => $pager->skipped,
            order_by => { 'id' => 'DESC' },
        }
    );
    my $cluster_api = $c->get('API::StorageCluster');
    foreach my $storage (@storages) {
        if ($storage->{cluster_id}) {
            $storage->{cluster} = $cluster_api->lookup( $storage->{cluster_id} );
        }
    }

    if ( @storages > $limit ) {
        $pager->total_entries( $limit * $pager->current_page + 1 );
        pop @storages;
    }
    my $stash = $c->stash;
    $stash->{storages} = \@storages;
    $stash->{pager} = $pager;
}

sub entities {
    my ($self, $c) = @_;

    my $storage_id = $c->match->{object_id};
    my $storage = $self->load_storage($c);
    my $object_id = $c->request->param('since') || 0;
    my $limit = 100;

    my $sql = <<EOSQL;
        SELECT
            e.object_id,
            CONCAT_WS( "/", b.name, o.name ) as object_url,
            o.internal_name,
            o.status as object_status,
            e.status as entity_status
        FROM object o
            JOIN bucket b on b.id = o.bucket_id
            JOIN entity e on o.id = e.object_id
        WHERE e.storage_id = ? AND object_id > ? LIMIT $limit
EOSQL

    my $dbh = $c->get('DB::Master');
    my $sth = $dbh->prepare( $sql );
    $sth->execute( $storage_id, $object_id );

    my $stash = $c->stash;
    $stash->{entities} = $sth->fetchall_arrayref({});
    $sth->finish;
}

sub add {
    my ($self, $c) = @_;
    $c->stash->{ clusters } = $c->get('API::StorageCluster')->search({});
}

sub add_post {
    my ($self, $c) = @_;

    my $params = $c->request->parameters->as_hashref;
    my $result = $self->validate( $c, storage_add => $params );
    if ($result->success) {
        my $valids = $result->valid;
        $c->get('API::Storage')->create( $valids );
        $c->redirect( $c->uri_for('/storage', {done => 1}) );
    } else {
        $c->stash->{template} = 'storage/add';
    }
    $c->stash->{ clusters } = $c->get('API::StorageCluster')->search({});
}

sub edit {
    my ($self, $c) = @_;

    my $storage = $self->load_storage($c);
    my %fill = (
        %$storage,
        capacity => STF::Utils::human_readable_size( $storage->{capacity} ),
    );
    if ( STF_ENABLE_STORAGE_META ) {
        my $meta = $storage->{meta};
        foreach my $meta_key ( keys %$meta ) {
            $fill{ "meta_$meta_key" } = $meta->{ $meta_key };
        }
    }
    $self->fillinform( $c, \%fill );
    $c->stash->{ clusters } = $c->get('API::StorageCluster')->search({});
}

sub edit_post {
    my ($self, $c) = @_;
    my $storage = $self->load_storage($c);

    my $params = $c->request->parameters->as_hashref;
    $params->{id} = $storage->{id};
    my $result = $self->validate( $c, storage_edit => $params );
    if ($result->success) {
        my $valids = $result->valid;
        my %meta;
        delete $valids->{id};
        if ( STF_ENABLE_STORAGE_META ) {
            foreach my $k ( keys %$valids ) {
                next if ( (my $sk = $k) !~ s/^meta_// );

                $meta{$sk} = delete $valids->{$k};
            }
        }
        my $api = $c->get('API::Storage');
        $api->update( $storage->{id} => $valids );
        if ( STF_ENABLE_STORAGE_META ) {
            $api->update_meta( $storage->{id}, \%meta );
        }

        $c->redirect( $c->uri_for( "/storage/list", { done => 1 } ) );
    } else {
        $c->stash->{template} = 'storage/edit';
        $self->fillinform( $c, $params );
    }
}

sub delete_post {
    my ($self, $c) = @_;

    my $storage = $self->load_storage($c);
    my $params = { id => $storage->{id} };
    my $result = $self->validate( $c, storage_delete => $params );
    if ( $result->success ) {
        $c->get('API::Storage')->delete( $storage->{id} );
        $c->redirect( $c->uri_for( '/storage/list', {done => 1}) );
    } else {
        $c->stash->{template} = 'storage/edit';
        $self->fillinform( $c, $params );
    }
}

no Mouse;

1;
