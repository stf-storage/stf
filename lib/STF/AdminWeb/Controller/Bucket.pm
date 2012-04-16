package STF::AdminWeb::Controller::Bucket;
use Mouse;
use JSON ();

extends 'STF::AdminWeb::Controller';

sub delete {
    my ($self, $c) = @_;
    my $bucket_id = $c->match->{bucket_id};

    $c->get('API::Bucket')->mark_for_delete( $bucket_id );
    $c->get('API::Queue')->enqueue( delete_bucket => $bucket_id );

    my $response = $c->response;
    $response->code( 200 );
    $response->content_type("application/json");
    $c->finished(1);

    $response->body(JSON::encode_json({ message => "bucket deleted" }));
}

sub list {
    my ($sef, $c) = @_;
    my $limit = 100;
    my $pager = $c->pager($limit);

    my %q;
    my $req = $c->request;
    if ( my $name = $req->param('name') ) {
        $q{name} = { LIKE => $name };
    }

    my @buckets = $c->get('API::Bucket')->search(
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

    my $stash = $c->stash;
    $stash->{pager} = $pager;
    $stash->{buckets} = \@buckets;
    $stash->{fdat} = $req->parameters->as_hashref;
}

sub view {
    my ($self, $c) = @_;

    my $bucket_id = $c->match->{bucket_id};
    my $bucket = $c->get('API::Bucket')->lookup( $bucket_id );
    my $total = $c->get('API::Object')->count({ bucket_id => $bucket_id });
    my $limit = 100;
    my $pager = $c->pager( $limit );

    my @objects = $c->get('API::Object')->search_with_entity_info(
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
    my $stash = $c->stash;
    $stash->{bucket} = $bucket;
    $stash->{objects} = \@objects;
    $stash->{pager} = $pager;
}

sub add {}
sub add_post {
    my ($self, $c) = @_;

    my $params = $c->request->parameters->as_hashref;
    my $result = $self->validate( $c, bucket_add => $params );
    if ($result->success) {
        my $stf_uri = $c->get('API::Config')->load_variable('stf.global.public_uri');
        my $valids = $result->valid;
        my $name = $valids->{name};
        my $furl = $c->get('Furl');
        my (undef, $code) = $furl->put( "$stf_uri/$name", [ 'Content-Length' => 0 ] );
        if ($code ne '201') {
            my $res = $c->response;
            $res->content_type('text/plain');
            $res->body( "Failed to create bucket at $stf_uri/$name" );
            $c->finished(1);
            return;
        }
        my $bucket = $c->get('API::Bucket')->lookup_by_name( $name );
        $c->redirect( $c->uri_for('/bucket/show', $bucket->{id}) );
    } else {
        $c->stash->{template} = 'bucket/add';
    }
    $c->stash->{ clusters } = $c->get('API::StorageCluster')->search({});
}

no Mouse;

1;