package STF::AdminWeb::Controller::Bucket;
use Mojo::Base 'STF::AdminWeb::Controller';

sub load_object {
    my ($self, $object_id) = @_;

    $object_id ||= $self->match->captures->{object_id};
    if ($object_id =~ /\D/) {
        # resolve bucket/path/to/object to object id
        $object_id = $self->resolve_public_name($object_id);
        if (! $object_id) {
            return;
        }
    }

    my $object = $self->get('API::Bucket')->lookup( $object_id );
    if (! $object) {
        return;
    }
    $self->stash(bucket => $object);
    return $object;
}

sub api_delete {
    my ($self) = @_;

    my $bucket = $self->load_object();
    if (! $bucket) {
        $self->render_json({}, status => 404);
        return;
    }

    $self->get('API::Bucket')->mark_for_delete({ id => $bucket->{id} });
    $self->get('API::Queue')->enqueue( delete_bucket => $bucket->{id} );

    $self->render_json({
        message => "bucket deleted"
    });
}

sub list {
    my ($self) = @_;
    my $limit = 100;
    my $pager = $self->pager($limit);

    my %q;
    my $req = $self->req;
    if ( my $name = $req->param('name') ) {
        $q{name} = { LIKE => $name };
    }

    my @buckets = $self->get('API::Bucket')->search(
        \%q,
        {
            limit    => $pager->entries_per_page + 1,
            offset   => $pager->skipped,
            order_by => { 'name' => 'ASC' },
        }
    );
    # fool pager
    if ( scalar @buckets > $limit ) {
        $pager->total_entries( $limit * $pager->current_page + 1 );
    }

    $self->fillinform($req->params->to_hash);
    $self->stash(
        pager => $pager,
        buckets => \@buckets
    );
}

sub view {
    my ($self) = @_;

    my $bucket = $self->load_object();
    if (! $bucket) {
        $self->render_not_found();
        return;
    }
    my $total = $self->get('API::Object')->count({ bucket_id => $bucket->{id} });
    my $limit = 100;
    my $pager = $self->pager( $limit );

    my @objects = $self->get('API::Object')->search_with_entity_info(
        { bucket_id => $bucket->{id} },
        {
            limit => $pager->entries_per_page + 1,
            offset => $pager->skipped,
            order_by => { 'name' => 'ASC' },
        }
    );

    if ( scalar @objects > $limit ) {
        $pager->total_entries( $limit * $pager->current_page + 1 );
    }
    $self->stash(
        bucket => $bucket,
        objects => \@objects,
        pager => $pager,
    );
}

sub add {}
sub add_post {
    my ($self) = @_;

    my $params = $self->req->params->to_hash;
    my $result = $self->validate(bucket_add => $params);
    if ($result->success) {
        my $stf_uri = $self->get('API::Config')->load_variable('stf.global.public_uri');
        my $valids = $result->valid;
        my $name = $valids->{name};
        my $furl = $self->get('Furl');
        my (undef, $code) = $furl->put( "$stf_uri/$name", [ 'Content-Length' => 0 ] );
        if ($code ne '201') {
            $self->render_text("Failed to create bucket at $stf_uri/$name");
            return;
        }
        my $bucket = $self->get('API::Bucket')->lookup_by_name( $name );
        $self->redirect_to( $self->url_for("/bucket/show/$bucket->{id}") );
    } else {
        $self->stash(template => 'bucket/add');
    }
    $self->stash(clusters => scalar $self->get('API::StorageCluster')->search({}));
}

1;