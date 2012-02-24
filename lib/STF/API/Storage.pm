package STF::API::Storage;
use strict;
use parent qw( STF::API::WithDBI );
use Guard ();
use STF::Constants qw(:storage STF_DEBUG);
use Class::Accessor::Lite
    new => 1,
    rw  => [ qw( enable_meta ) ]
;

my @META_KEYS = qw(used capacity notes);

# XXX These queries to load meta info should, and can be optimized
sub search {
    my ($self, @args) = @_;
    my $list = $self->SUPER::search(@args);
    if ($self->enable_meta) {
        my $meta_api = $self->get('API::StorageMeta');
        foreach my $object ( @$list ) {
            my ($meta) = $meta_api->search({ storage_id => $object->{id} });
            $object->{meta} = $meta;
        }
    }
    return wantarray ? @$list : $list;
}

sub lookup {
    my ($self, $id) = @_;
    my $object = $self->SUPER::lookup($id);
    if ($object && $self->enable_meta) {
        my ($meta) = $self->get('API::StorageMeta')->search({
            storage_id => $object->{id}
        });
        $object->{meta} = $meta;
    }
    return $object;
}

sub create {
    my ($self, $args) = @_;

    my %meta_args;
    foreach my $key ( @META_KEYS ) {
        $meta_args{$key} = delete $args->{$key};
    }

    my $object = $self->SUPER::create($args);
    if ( $self->enable_meta ) {
        my $meta = $self->get('API::StorageMeta')->create({
            %meta_args,
            storage_id => $object->{id},
        });
        $object->{meta} = $meta;
    }
    return $object;
}

sub update {
    my ($self, $id, $args) = @_;

    my %meta_args;
    foreach my $key ( @META_KEYS ) {
        $meta_args{$key} = delete $args->{$key};
    }

    my $rv = $self->SUPER::update($id, $args);
    if ( $self->enable_meta ) {
        my $meta = $self->get('API::StorageMeta')->create({
            %meta_args,
            storage_id => $id,
        });
    }
    return $rv;
}

sub update_usage_for_all {
    my $self = shift;

    my $dbh = $self->dbh;
    my $sth = $dbh->prepare( "SELECT id FROM storage" );
    $sth->execute();
    my $storage_id;
    $sth->bind_columns( \$storage_id );
    while ( $sth->fetchrow_arrayref ) {
        $self->update_usage( $storage_id );
    }
}

sub update_usage {
    my( $self, $storage_id ) = @_;

    if (STF_DEBUG) {
        printf STDERR "[    Usage] Updating usage for storage %s\n",
            $storage_id
        ;
    }
    my $dbh = $self->dbh;
    my($used) = $dbh->selectrow_array( <<EOSQL, undef, $storage_id ) || 0;
        SELECT SUM(o.size)
            FROM object o JOIN entity e ON o.id = e.object_id
            WHERE e.storage_id = ?
EOSQL

    $dbh->do( <<EOSQL, undef, $used, $storage_id );
        UPDATE storage_meta SET used = ? WHERE storage_id = ?
EOSQL
    $self->lookup( $storage_id ); # for cache
    return $used;
}


sub move_entities {
    my ($self, $storage_id) = @_;

    my $dbh = $self->dbh;
    my $sth = $dbh->prepare( <<EOSQL );
        SELECT e.*, CONCAT_WS('/', s.uri, o.internal_name) as url
            FROM entity e INNER JOIN storage s ON e.storage_id = s.id
                          INNER JOIN object  o ON e.object_id  = o.id
            WHERE s.id = ?
            LIMIT ?
EOSQL
    my $limit = 10_000;
    my $skipped = 0;
    my $processed = 0;
    my %invalid;
    my $entity_api = $self->get( 'API::Entity');
    while ( 1 ) {
        my $rv = $sth->execute( $storage_id, $limit );
        while ( my $entity = $sth->fetchrow_hashref ) {
            $processed++;
            my $replicated = $entity_api->replicate( { object_id => $entity->{object_id} } ) || 0;
            if ( $replicated > 0 ) {
                if (STF_DEBUG) {
                    printf STDERR "[      Move] Deleting entity storage = %s, object = %s\n",
                        $entity->{storage_id},
                        $entity->{object_id}
                    ;
                }
                $entity_api->delete({
                    storage_id => $entity->{storage_id},
                    object_id  => $entity->{object_id},
                });
            } else {
                # XXX これ、object_idだけでいいんじゃ？
                my $key = join '-', @$entity{ qw(storage_id object_id) };
                if ( ! $invalid{$key}++) {
                    if ( STF_DEBUG ) {
                        printf STDERR "[      Move] Failed to replicate object_id %s on storage_id %s\n",
                            @$entity{ qw(object_id storage_id) };
                    }
                    $skipped++;
                }
            }
        }
        last if ( $rv < $limit );
    }

    return $processed;
}

sub retire {
    my ($self, $storage_id) = @_;
    if ( STF_DEBUG ) {
        printf STDERR "[    Retire] Retiring storage %s\n",
            $storage_id
        ;
    }
    $self->update( $storage_id, { mode => STORAGE_MODE_MIGRATE_NOW } );

    my $guard = Guard::guard {
        $self->update( $storage_id, { mode => STORAGE_MODE_RETIRE } );
    };

    my $processed = $self->move_entities( $storage_id );

    $guard->cancel;

    if (STF_DEBUG) {
        printf STDERR "[    Retire] Storage %d, processed %d rows\n",
            $storage_id, $processed;
    }
    $self->update( $storage_id => { mode => STORAGE_MODE_MIGRATED } );
}

sub find_and_retire {
    my $self = shift;

    my $dbh = $self->dbh;

    my @storages = $self->search( { mode => STORAGE_MODE_RETIRE } );
    foreach my $storage ( @storages ) {
        $self->retire( $storage->{id} );
    }

    if (STF_DEBUG) {
        printf STDERR "[    Retire] Retired %d storages\n",
            scalar @storages;
    }
}

sub recover_crash {
    my ($self, $storage_id) = @_;

    if ( STF_DEBUG ) {
        printf STDERR "[     Crash] Recovering storage %s\n",
            $storage_id
        ;
    }
    $self->update( $storage_id, { mode => STORAGE_MODE_CRASH_RECOVER_NOW } );

    my $guard = Guard::guard {
        $self->update( $storage_id, { mode => STORAGE_MODE_CRASH } );
    };

    my $processed = $self->move_entities( $storage_id );

    $guard->cancel;

    if (STF_DEBUG) {
        printf STDERR "[     Crash] Storage %d, processed %d rows\n",
            $storage_id, $processed;
    }
    $self->update( $storage_id => { mode => STORAGE_MODE_CRASH_RECOVERED } );
}

sub find_and_recover_crash {
    my $self = shift;

    my @storages = $self->search( { mode => STORAGE_MODE_CRASH  } );

    foreach my $storage ( @storages ) {
        $self->recover_crash( $storage->{id} );
    }

    if (STF_DEBUG) {
        printf STDERR "[     Crash] Recovered %d storages\n",
            scalar @storages;
    }
}

1;
