package STF::AdminWeb::Controller::Bucket;
use Mojo::Base 'STF::AdminWeb::Controller';

sub delete {
    my ($self) = @_;
    my $bucket_id = $self->match->captures->{bucket_id};

    $self->get('API::Bucket')->mark_for_delete({ id => $bucket_id });
    $self->get('API::Queue')->enqueue( delete_bucket => $bucket_id );

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

    my $bucket_id = $self->match->captures->{bucket_id};
    my $bucket = $self->get('API::Bucket')->lookup( $bucket_id );
    my $total = $self->get('API::Object')->count({ bucket_id => $bucket_id });
    my $limit = 100;
    my $pager = $self->pager( $limit );

    my @objects = $self->get('API::Object')->search_with_entity_info(
        { bucket_id => $bucket_id },
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