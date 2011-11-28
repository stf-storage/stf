use strict;
use STF::Utils;
use Regexp::Common qw(URI);

return +{
    storage_add => {
        required => [qw(id uri capacity mode)],
        optional => [qw(used)],
        defaults => {
            used => 0,
        },
        field_filters => {
            uri => sub {
                my $uri = shift;
                $uri =~ s{/*$}{};
                $uri;
            },
            capacity => \&STF::Utils::as_bytes,
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
        required => [qw(id uri capacity mode)],
        field_filters => {
            uri => sub {
                my $uri = shift;
                $uri =~ s{/*$}{};
                $uri;
            },
            capacity => \&STF::Utils::as_bytes,
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
};


