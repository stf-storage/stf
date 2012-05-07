package STF::API::Storage;
use Mouse;
use Guard ();
use STF::Constants qw(:storage STF_DEBUG STF_ENABLE_STORAGE_META);
use STF::Log;

with 'STF::API::WithDBI';

my @META_KEYS = qw(used capacity notes);

sub is_writable {
    my ($self, $storage) = @_;

    my $mode = $storage->{mode};

    # XXX The storage is writeable in the following cases:
    #    1) STORAGE_MODE_READ_WRITE:
    #       captain obvious
    #    2) STORAGE_MODE_SPARE:
    #       because this is just a spare in case something happens
    #    3) STORAGE_MODE_REPAIR(_NOW|_DONE)?:
    #       'repair' storages are ones that are being repaired, but are
    #       not necessarily broken. (if it's broken, you should just 'crash' it)
    return
        $mode == STORAGE_MODE_READ_WRITE  ||
        $mode == STORAGE_MODE_SPARE       ||
        $mode == STORAGE_MODE_REPAIR      ||
        $mode == STORAGE_MODE_REPAIR_DONE ||
        $mode == STORAGE_MODE_REPAIR_NOW
    ;
}

sub is_readable {
    my ($self, $storage) = @_;
    my $mode = $storage->{mode};
    return 
        $mode == STORAGE_MODE_READ_ONLY   ||
        $mode == STORAGE_MODE_READ_WRITE  ||
        $mode == STORAGE_MODE_SPARE       ||
        $mode == STORAGE_MODE_REPAIR      ||
        $mode == STORAGE_MODE_REPAIR_NOW  ||
        $mode == STORAGE_MODE_REPAIR_DONE
}

# XXX These queries to load meta info should, and can be optimized
around search => sub {
    my ($next, $self, @args) = @_;
    my $list = $self->$next(@args);
    if ( STF_ENABLE_STORAGE_META ) {
        my $meta_api = $self->get('API::StorageMeta');
        foreach my $object ( @$list ) {
            my ($meta) = $meta_api->search({ storage_id => $object->{id} });
            $object->{meta} = $meta;
        }
    }
    return wantarray ? @$list : $list;
};

around lookup => sub {
    my ($next, $self, $id) = @_;
    my $object = $self->$next($id);
    if ( STF_ENABLE_STORAGE_META ) {
        my ($meta) = $self->get('API::StorageMeta')->search({
            storage_id => $object->{id}
        });
        $object->{meta} = $meta;
    }
    return $object;
};

around create => sub {
    my ($next, $self, $args) = @_;

    my %meta_args;
    if ( STF_ENABLE_STORAGE_META ) {
        foreach my $key ( @META_KEYS ) {
            if (exists $args->{$key} ) {
                $meta_args{$key} = delete $args->{$key};
            }
        }
    }

    my $rv = $self->$next($args);

    if ( STF_ENABLE_STORAGE_META ) {
        $self->get('API::StorageMeta')->create({
            %meta_args,
            storage_id => $args->{id},
        });
    }
    return $rv;
};

sub load_writable_for {
    my ($self, $args) = @_;

    my $cluster = $args->{cluster} or die "XXX no cluster";
    my $object  = $args->{object}  or die "XXX no object";
    my $dbh = $self->dbh;
    my @writable_modes = (
       STORAGE_MODE_READ_WRITE,
       STORAGE_MODE_SPARE,
       STORAGE_MODE_REPAIR,
       STORAGE_MODE_REPAIR_NOW,
       STORAGE_MODE_REPAIR_DONE,
    );
    my $storages = $dbh->selectall_arrayref(<<EOSQL, { Slice => {} }, @writable_modes, $cluster->{id}, $object->{id});
        SELECT s.* FROM storage s
            WHERE s.mode in(@{[ join ', ', map { '?' } @writable_modes ]}) AND s.cluster_id = ? AND s.id NOT IN
                (SELECT storage_id FROM entity WHERE object_id = ?)
        ORDER BY rand()
EOSQL

    return $storages;
}

sub update_meta {
    if ( STF_ENABLE_STORAGE_META ) {
        my ($self, $storage_id, $args) = @_;
        my $rv = $self->get('API::StorageMeta')->update_for( $storage_id, $args );
        return $rv;
    }
}

around update => sub {
    my ($next, $self, $id, $args) = @_;
    my $rv = $self->$next($id, $args);
    if (! ref $id ) {
        my $storage = $self->lookup( $id );
        $self->cache_delete( storage_cluster => $storage->{cluster_id} );
    }
    return $rv;
};

1;
