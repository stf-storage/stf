package STF::API::Storage;
use strict;
use parent qw( STF::API::WithDBI );
use Guard ();
use STF::Constants qw(:storage STF_DEBUG STF_ENABLE_STORAGE_META);
use Class::Accessor::Lite
    new => 1,
;

my @META_KEYS = qw(used capacity notes);

# XXX These queries to load meta info should, and can be optimized
sub search {
    my ($self, @args) = @_;
    my $list = $self->SUPER::search(@args);
    if ( STF_ENABLE_STORAGE_META ) {
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
    if ( STF_ENABLE_STORAGE_META ) {
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
    if ( STF_ENABLE_STORAGE_META ) {
        foreach my $key ( @META_KEYS ) {
            if (exists $args->{$key} ) {
                $meta_args{$key} = delete $args->{$key};
            }
        }
    }

    my $rv = $self->SUPER::create($args);

    if ( STF_ENABLE_STORAGE_META ) {
        $self->get('API::StorageMeta')->create({
            %meta_args,
            storage_id => $args->{id},
        });
    }
    return $rv;
}

sub update_meta {
    if ( STF_ENABLE_STORAGE_META ) {
        my ($self, $storage_id, $args) = @_;
        my $rv = $self->get('API::StorageMeta')->update_for( $storage_id, $args );
        return $rv;
    }
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
        SELECT e.object_id FROM entity e FORCE INDEX (PRIMARY)
            JOIN object o ON e.object_id = o.id
            WHERE e.storage_id = ? AND e.object_id > ? LIMIT ?
EOSQL
    my $limit = 10_000;
    my $processed = 0;
    my $object_id = 0;
    my $queue_api = $self->get( 'API::Queue');
    while ( 1 ) {
        my $rv = $sth->execute( $storage_id, $object_id, $limit );
        last if $rv <= 0;
        $sth->bind_columns( \($object_id ) );
        while ( $sth->fetchrow_arrayref ) {
            $processed++;
            if ( STF_DEBUG ) {
                printf STDERR "[   Storage] Sending object %s to repair queue\n",
                    $object_id,
                ;
            }
            $queue_api->enqueue( repair_object => $object_id );
        }
        if ( $limit == $rv ) {
            if ( STF_DEBUG ) {
                printf STDERR "[   Storage] Sent %d objects, sleeping to give it some time...\n",
                    $limit
                ;
            }
            sleep 60;
        }
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
