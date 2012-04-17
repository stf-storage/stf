use strict;
use STF::Utils;
use Regexp::Common qw(URI);

return +{
    storage_add => {
        required => [qw(id uri mode)],
        optional => [qw(meta_used meta_notes meta_capacity cluster_id)],
        field_filters => {
            uri => sub {
                my $uri = shift;
                if ($uri !~ /^http:\/\//)  {
                    $uri = "http://$uri";
                }
                $uri =~ s{/*$}{};
                $uri;
            },
            meta_capacity => \&STF::Utils::as_bytes,
        },
        constraint_methods => {
            uri => [
                qr{$RE{URI}{HTTP}},
                {
                    name => 'duplicate',
                    constraint_method => sub {
                        my( $dfv, $uri ) = @_;
                        my($row) = $dfv->container->get('API::Storage')->search({
                            uri => $uri 
                        });
                        return !$row;
                    },
                },
            ],
            id => {
                name => 'duplicate',
                constraint_method => sub {
                    my( $dfv, $id ) = @_;
                    my $row = $dfv->container->get('API::Storage')->lookup( $id );
                    return !$row;
                },
            }

        },
    },
    storage_edit => {
        required => [qw(id uri mode)],
        optional => [qw(meta_used meta_notes meta_capacity cluster_id)],
        field_filters => {
            uri => sub {
                my $uri = shift;
                $uri =~ s{/*$}{};
                $uri;
            },
            meta_capacity => \&STF::Utils::as_bytes,
        },
        constraint_methods => {
            uri => qr{$RE{URI}{HTTP}},
            id => {
                name => 'duplicate',
                params => [ qw(id uri) ],
                constraint_method => sub {
                    my( $dfv, $id, $uri ) = @_;
                    my($row) = $dfv->container->get('API::Storage')->search({
                        uri => $uri,
                    });
                    return 1 unless $row;
                    return 1 if $row->{id} eq $id;
                    return 0;
                },
            },
        },
    },
    storage_delete => {
        required => [qw(id)],
        constraint_methods => {
            id => {
                name => 'has_entities',
                constraint_method => sub {
                    my( $dfv, $id ) = @_;
                    my( $row ) = $dfv->container->get('API::Entity')->search({
                        storage_id => $id,
                    }, {
                        limit => 1,
                    });
                    return 0 if $row;
                    return 1;
                },
            },
        },
    },
    cluster_add => {
        required => [qw(id mode)],
        optional => [qw(name)],
        constraint_methods => {
            id => {
                name => 'duplicate',
                constraint_method => sub {
                    my( $dfv, $id ) = @_;
                    my $row = $dfv->container->get('API::StorageCluster')->lookup( $id );
                    return !$row;
                },
            }

        },
    },
    cluster_edit => {
        required => [qw(id mode)],
        optional => [qw(name)],
    },
    cluster_delete => {
        required => [qw(id)],
    },
    bucket_add => {
        required => [qw(name)],
        constraint_methods => {
            name => {
                name => 'duplicate',
                constraint_method => sub {
                    my( $dfv, $name ) = @_;
                    my $row = $dfv->container->get('API::Bucket')->lookup_by_name( $name );
                    return !$row;
                },
            }
        }
    },
    object_create => {
        required => [qw(bucket_name object_name )]
    },
    object_edit => {
        required => [qw(id num_replica status)],
        optional => [qw(cluster_id)],
    }
};


