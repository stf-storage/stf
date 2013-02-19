package STF::AdminWeb::Controller::Storage;
use Mojo::Base 'STF::AdminWeb::Controller';
use STF::Utils;
use STF::Constants qw(
    STF_ENABLE_STORAGE_META
);

sub api_list {
    my ($self) = @_;

    my @storages = $self->get('API::Storage')->search(
        {},
        {
            order_by => { id => 'DESC' }
        }
    );
    $self->render_json({
        storages => \@storages
    });
}

sub load_storage {
    my ($self) = @_;
    my $storage_id = $self->match->captures->{object_id};
    my $storage = $self->get('API::Storage')->lookup( $storage_id );
    if (! $storage) {
        return;
    }
    $self->stash(storage => $storage);
    return $storage;
}

sub list {
    my ($self) = @_;
    my $limit = $self->req->param('limit') || 100;
    my $pager = $self->pager( $limit );
    my @storages = $self->get( 'API::Storage' )->search(
        {},
        {
            limit    => $pager->entries_per_page + 1,
            offset   => $pager->skipped,
            order_by => { 'id' => 'DESC' },
        }
    );
    my $cluster_api = $self->get('API::StorageCluster');
    foreach my $storage (@storages) {
        if ($storage->{cluster_id}) {
            $storage->{cluster} = $cluster_api->lookup( $storage->{cluster_id} );
        }
    }

    if ( @storages > $limit ) {
        $pager->total_entries( $limit * $pager->current_page + 1 );
        pop @storages;
    }
    $self->stash(
        storages => \@storages,
        pager    => $pager
    );
}

sub entities {
    my ($self) = @_;

    my $storage = $self->load_storage();
    if (! $storage) {
        $self->render_not_found();
        return;
    }
    my $object_id = $self->req->param('since') || 0;
    my $limit = 100;

    my @entities = 
    my $dbh = $self->get('DB::Master');
    my $entities = $dbh->selectall_arrayref(<<EOSQL, { Slice => {} }, $storage_id, $object_id );
        SELECT * FROM entity
            WHERE storage_id = ? AND object_id > ?
            ORDER BY object_id ASC
            LIMIT $limit
EOSQL
    my $sql = <<EOSQL;
        SELECT
            CONCAT_WS( "/", b.name, o.name ) as object_url,
            o.internal_name,
            o.status as object_status
        FROM object o
            JOIN bucket b on b.id = o.bucket_id
        WHERE o.id = ?
EOSQL
    my $sth = $dbh->prepare( $sql );
    my ($object_url, $internal_name, $object_status);
    foreach my $entity ( @$entities ) {
        $sth->execute( $entity->{object_id} );
        $sth->bind_columns( \($object_url, $internal_name, $object_status) );
        if ($sth->fetchrow_arrayref) {
            $entity->{object_url} = $object_url;
            $entity->{internal_name} = $internal_name;
            $entity->{object_status} = $object_status;
        }
        $sth->finish;
    }

    $self->stash(entities => $entities);
}

sub add {
    my ($self) = @_;
    $self->stash(clusters => scalar $c->get('API::StorageCluster')->search({}));
}

sub add_post {
    my ($self) = @_;

    my $params = $self->req->params->to_hash;
    my $result = $self->validate( storage_add => $params );
    if ($result->success) {
        my $valids = $result->valid;
        foreach my $key (keys %$valids) {
            if ($key =~ /^meta_(.+)$/) {
                $valids->{$1} = delete $valids->{$key};
            }
        }
        $self->get('API::Storage')->create( $valids );
        $self->redirect_to( $c->url_for('/storage', {done => 1}) );
    } else {
        $self->stash(template => 'storage/add');
    }
    $self->stash(clusters => scalar $self->get('API::StorageCluster')->search({}));
}

sub edit {
    my ($self) = @_;

    my $storage = $self->load_storage();
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
    $self->fillinform( \%fill );
    $self->stash(clusters => scalar $c->get('API::StorageCluster')->search({}));
}

sub edit_post {
    my ($self) = @_;
    my $storage = $self->load_storage();
    if (! $storage) {
        $self->render_not_found;
        return;
    }

    my $params = $self->req->parameters->as_hashref;
    $params->{id} = $storage->{id};
    my $result = $self->validate( storage_edit => $params );
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
        my $api = $self->get('API::Storage');
        $api->update( $storage->{id} => $valids );
        if ( STF_ENABLE_STORAGE_META ) {
            $api->update_meta( $storage->{id}, \%meta );
        }

        $self->redirect_to( $self->url_for( "/storage/list", { done => 1 } ) );
    } else {
        $self->stash(template => 'storage/edit');
        $self->fillinform( $params );
    }
}

# XXX Should make this an API call?
sub delete_post {
    my ($self) = @_;

    my $storage = $self->load_storage();
    if (! $storage) {
        $self->render_not_found;
        return;
    }

    my $params = { id => $storage->{id} };
    my $result = $self->validate( storage_delete => $params );
    if ( $result->success ) {
        $self->get('API::Storage')->delete( $storage->{id} );
        $self->redirect_to( $self->url_for( '/storage/list', {done => 1}) );
    } else {
        $self->stash(template => 'storage/edit');
        $self->fillinform( $params );
    }
}

1;
