package STF::AdminWeb::Controller::Object;
use Mouse;
use JSON ();
use STF::Constants qw(STF_ENABLE_OBJECT_META);

extends 'STF::AdminWeb::Controller';

sub load_object {
    my ($self, $c, $object_id) = @_;

    $object_id ||= $c->match->{object_id};
    my $object = $c->get('API::Object')->lookup( $object_id );
    if (! $object) {
        $c->response->status(404);
        $c->abort;
    }
    $c->stash->{object} = $object;
    return $object;
}

sub view_public_name {
    my ($self, $c) = @_;
    my ($bucket_name, $object_name) = @{ $c->match->{splat} || [] };

    my $bucket = $c->get('API::Bucket')->lookup_by_name($bucket_name);
    if (! $bucket) {
        my $response = $c->response;
        $response->status(404);
        $c->abort();
    }

    my $object_id = $c->get('API::Object')->find_object_id( {
        bucket_id => $bucket->{id},
        object_name => $object_name
    } );
    
    if (! $object_id) {
        my $response = $c->response;
        $response->status(404);
        $c->abort();
    }

    $c->match->{object_id} = $object_id;
    $c->stash->{template} = 'object/view';
    $self->view($c);
}

sub view {
    my ($self, $c) = @_;

    my $object = $self->load_object($c);
    if (! $object) {
        return;
    }

    my $bucket = $c->get('API::Bucket')->lookup( $object->{bucket_id} );
    $object->{cluster} = $c->get('API::StorageCluster')->load_for_object( $object->{id} );

    my $limit = 100;
    my $pager = $c->pager($limit);

    my @entities = $c->get('API::Entity')->search_with_url(
        { object_id => $object->{id} },
        { order_by => \'status DESC' },
    );

    my $stash = $c->stash;
    $stash->{bucket} = $bucket;
    $stash->{object} = $object;
    $stash->{pager} = $pager;
    $stash->{entities} = \@entities;
}

sub repair {
    my ($self, $c) = @_;

    my $object = $self->load_object( $c );
    if (! $object) {
        return;
    }
    $c->get('API::Queue')->enqueue(repair_object => $object->{id});

    my $response = $c->response;
    $response->code( 200 );
    $response->content_type("application/json");
    $c->finished(1);

    $response->body(JSON::encode_json({ message => "object enqueued for repair" }));
}

sub delete {
    my ($self, $c) = @_;
    my $object = $self->load_object( $c );
    if (! $object) {
        return;
    }

    $c->get('API::Object')->mark_for_delete( $object->{id} );
    $c->get('API::Queue')->enqueue( delete_object => $object->{id} );

    my $response = $c->response;
    $response->code( 200 );
    $response->content_type("application/json");
    $c->finished(1);

    $response->body(JSON::encode_json({ message => "object deleted" }));
}

sub create {
    my ($self, $c) = @_;

    if (my $bucket_id = $c->request->param('bucket_id')) {
        my $bucket = $c->get('API::Bucket')->lookup( $bucket_id );
        $self->fillinform( $c, { 
            map { ( "bucket_$_" => $bucket->{$_} ) } keys %$bucket
        });
    }
}

sub create_post {
    my ($self, $c) = @_;

    my $params = $c->request->parameters->as_hashref;
    my $upload = $c->request->uploads->{content};
    my $result = $self->validate( $c, object_create => $params );
    if ($result->success && $upload) {
        my $stf_uri = $c->get('API::Config')->load_variable('stf.global.public_uri');
        my $valids = $result->valid;
        my $bucket = $c->get('API::Bucket')->lookup_by_name( $valids->{bucket_name} );
        my $object_name = $valids->{object_name};
        open my $fh, '<', $upload->path;

        my $furl = $c->get('Furl');
        my (undef, $code, $msg, $hdrs, $content) = $furl->put(
            "$stf_uri/$bucket->{name}/$object_name",
            [ "Content-Length" => $upload->size ],
            $fh,
        );
        if ( $code ne 201 ) {
            my $res = $c->response;
            $res->content_type('text/plain');
            $res->body( "Failed to create object at $stf_uri/$bucket->{name}/$object_name ($code)" );
            $c->finished(1);
            return;
        }

        my $object_id = $c->get('API::Object')->find_object_id({
            bucket_id => $bucket->{id},
            object_name => $object_name
        });
        $c->redirect( $c->uri_for('/object/show', $object_id) );
        return;
    }
    $c->stash->{template} = 'object/create';
}

sub edit {
    my ($self, $c) = @_;
    my $object_id = $c->match->{object_id};
    my $object = $self->load_object($c, $object_id);
    if (! $object) {
        return;
    }
    $object->{cluster} = $c->get('API::StorageCluster')->load_for_object( $object->{id} );
    $c->stash->{clusters} = $c->get('API::StorageCluster')->search({});
    $self->fillinform( $c, {
        %$object,
        cluster_id => $object->{cluster}->{id},
    });
}

sub edit_post {
    my ($self, $c) = @_;
    my $object = $self->load_object($c);

    my $params = $c->request->parameters->as_hashref;
    $params->{id} = $object->{id};
    my $result = $self->validate( $c, object_edit => $params );
    if ($result->success) {
        my $valids = $result->valid;
        my %meta;
        delete $valids->{id};

        my $cluster_id = delete $valids->{cluster_id};
        if ( $cluster_id ) {
            $c->get('API::StorageCluster')->register_for_object({
                object_id => $object->{id},
                cluster_id => $cluster_id
            });
        }

        if ( STF_ENABLE_OBJECT_META ) {
            foreach my $k ( keys %$valids ) {
                next if ( (my $sk = $k) !~ s/^meta_// );

                $meta{$sk} = delete $valids->{$k};
            }
        }
        my $api = $c->get('API::Object');
        $api->update( $object->{id} => $valids );
        if ( STF_ENABLE_OBJECT_META ) {
            $api->update_meta( $object->{id}, \%meta );
        }

        $c->redirect( $c->uri_for( "/object/show", $object->{id} ) );
    } else {
        $c->stash->{template} = 'object/edit';
        $self->fillinform( $c, $params );
    }
}

no Mouse;

1;