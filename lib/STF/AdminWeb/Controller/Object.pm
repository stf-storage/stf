package STF::AdminWeb::Controller::Object;
use Mojo::Base 'STF::AdminWeb::Controller';
use JSON ();
use STF::Constants qw(STF_ENABLE_OBJECT_META);

sub load_object {
    my ($self, $object_id) = @_;

    $object_id ||= $self->match->captures->{object_id};
    if ($object_id =~ /\D/) {
        # resolve bucket/path/to/object to object id
        $object_id = $self->resolve_public_name($object_id);
        if (! $object_id) {
            $self->render_not_found;
            return;
        }
    }

    my $object = $self->get('API::Object')->lookup( $object_id );
    if (! $object) {
        $self->render_not_found;
        return;
    }
    $self->stash(object => $object);
    return $object;
}

sub resolve_public_name {
    my ($self, $name) = @_;

    my ($bucket_name, $object_name) = split(/\//, $name, 2);
    my $bucket = $self->get('API::Bucket')->lookup_by_name($bucket_name);
    if (! $bucket) {
        return;
    }

    my $object_id = $self->get('API::Object')->find_object_id( {
        bucket_id => $bucket->{id},
        object_name => $object_name
    } );
    $self->stash(bucket => $bucket);

    return $object_id;
}

sub view {
    my ($self) = @_;

    my $object = $self->load_object();
    if (! $object) {
        return;
    }

    my $bucket = $self->stash->{bucket} || 
        $self->get('API::Bucket')->lookup( $object->{bucket_id} );
    $object->{cluster} = $self->get('API::StorageCluster')->load_for_object( $object->{id} );

    my $limit = 100;
    my $pager = $self->pager($limit);

    my @entities = $self->get('API::Entity')->search_with_url(
        { object_id => $object->{id} },
        { order_by => \'status DESC' },
    );

    $self->stash(
        bucket      => $bucket,
        object      => $object,
        pager       => $pager,
        entities    => \@entities,
    )
}

# XXX Need to change this to /api/...
sub repair {
    my ($self) = @_;

    my $object = $self->load_object();
    if (! $object) {
        return;
    }
    $self->get('API::Queue')->enqueue(repair_object => $object->{id});

    $self->render_json({
        message => "object enqueued for repair"
    });
}

sub delete {
    my ($self) = @_;
    my $object = $self->load_object();
    if (! $object) {
        return;
    }

    $self->get('API::Object')->mark_for_delete( $object->{id} );
    $self->get('API::Queue')->enqueue( delete_object => $object->{id} );

    $self->render_json({
        message => "object deleted"
    });
}

sub create {
    my ($self) = @_;

    if (my $bucket_id = $self->request->param('bucket_id')) {
        my $bucket = $self->get('API::Bucket')->lookup( $bucket_id );
        $self->fillinform({ 
            map { ( "bucket_$_" => $bucket->{$_} ) } keys %$bucket
        });
    }
}

sub create_post {
    my ($self) = @_;

    my $params = $self->request->parameters->as_hashref;
    my $upload = $self->request->uploads->{content};
    my $result = $self->validate(object_create => $params );
    if ($result->success && $upload) {
        my $stf_uri = $self->get('API::Config')->load_variable('stf.global.public_uri');
        my $valids = $result->valid;
        my $bucket = $self->get('API::Bucket')->lookup_by_name( $valids->{bucket_name} );
        my $object_name = $valids->{object_name};
        open my $fh, '<', $upload->path;

        my $furl = $self->get('Furl');
        my (undef, $code, $msg, $hdrs, $content) = $furl->put(
            "$stf_uri/$bucket->{name}/$object_name",
            [ "Content-Length" => $upload->size ],
            $fh,
        );
        if ( $code ne 201 ) {
            $self->render_text(
                "Failed to create object at $stf_uri/$bucket->{name}/$object_name ($code)"
            );
            return;
        }

        my $object_id = $self->get('API::Object')->find_object_id({
            bucket_id => $bucket->{id},
            object_name => $object_name
        });
        $self->redirect_to( $self->url_for('/object/show', $object_id) );
        return;
    }
    $self->stash(template => 'object/create');
}

sub edit {
    my ($self) = @_;
    my $object = $self->load_object();
    if (! $object) {
        return;
    }
    $object->{cluster} = $self->get('API::StorageCluster')->load_for_object( $object->{id} );
    $self->stash(clusters => $self->get('API::StorageCluster')->search({}));
    $self->fillinform({
        %$object,
        cluster_id => $object->{cluster}->{id},
    });
}

sub edit_post {
    my ($self) = @_;
    my $object = $self->load_object();

    my $params = $self->request->parameters->as_hashref;
    $params->{id} = $object->{id};
    my $result = $self->validate(object_edit => $params );
    if ($result->success) {
        my $valids = $result->valid;
        my %meta;
        delete $valids->{id};

        my $cluster_id = delete $valids->{cluster_id};
        if ( $cluster_id ) {
            $self->get('API::StorageCluster')->register_for_object({
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
        my $api = $self->get('API::Object');
        $api->update( $object->{id} => $valids );
        if ( STF_ENABLE_OBJECT_META ) {
            if ( keys %meta > 0 ) {
                $api->update_meta( $object->{id}, \%meta );
            }
        }

        $self->redirect_to( $self->url_for( "/object/show", $object->{id} ) );
    } else {
        $self->stash(template => 'object/edit');
        $self->fillinform( $params );
    }
}

no Mouse;

1;