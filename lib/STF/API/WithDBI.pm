package STF::API::WithDBI;
use Mouse::Role;
use Scalar::Util ();
use SQL::Maker;

with qw(
    STF::Trait::WithCache
    STF::Trait::WithDBI
);

has sql_maker => (
    is => 'rw',
    lazy => 1,
    builder => 'build_sql_maker'
);

has table => (
    is => 'rw',
    lazy => 1,
    builder => 'build_table'
);

sub build_sql_maker {
    my $self = shift;
    return SQL::Maker->new( driver => $self->dbh->{Driver}->{Name} );
}

sub build_table {
    my $self = shift;
    my $table = (split /::/, Scalar::Util::blessed $self)[-1];
    $table =~ s/([a-z0-9])([A-Z])/$1_$2/g;
    return lc $table;
    
}

sub lookup {
    my ($self, $id) = @_;

    my $obj = ! ref $id && $self->cache_get($self->table => $id);
    if ($obj) {
        return $obj;
    }

    my ($sql, @binds) = $self->sql_maker->select( $self->table, [ '*' ], { id => $id });
    $obj = $self->dbh->selectrow_hashref($sql, undef, @binds);
    if ($obj) {
        $self->cache_set( [ $self->table => $id ], $obj );
    }
    return $obj;
}

sub lookup_multi {
    my ($self, @ids) = @_;

    my %keys = map {
        ( $self->cache_key($self->table => $_) => $_ )
    } @ids;
    my $cached = $self->cache_get_multi(keys %keys);
    my %result;
    foreach my $key (keys %keys) {
        my $value = $cached->{$key};
        if (defined $value) {
            $result{ $keys{$key} } = $value;
        } else {
            $result{ $keys{$key} } = $self->lookup( $keys{$key} );
        }
    }
    return \%result;
}

sub search {
    my ($self, $where, $opts) = @_;
    my ($sql, @binds) = $self->sql_maker->select( $self->table, [ '*' ], $where, $opts );
    my $results =  $self->dbh->selectall_arrayref( $sql, { Slice => {} }, @binds );
    return wantarray ? @$results : $results;
}

sub create {
    my ($self, $args, $opts) = @_;

    $opts ||= {};
    my ($sql, @binds) = $self->sql_maker->insert(
        $self->table,
        $args,
        $opts,
    );
    return $self->dbh->do($sql, undef, @binds);
}

sub update {
    my ($self, $id, $args, $where) = @_;

    $where ||= {};
    if (my $ref = ref $id) {
        if ($ref eq 'HASH') {
            $where = { %$where, %$id };
        }
        if ( my $pk = $where->{id} ) {
            $self->cache_delete( $self->table => $pk );
        }
    } else {
        $self->cache_delete( $self->table => $id );
        $where->{id} = $id;
    }

    my ($sql, @binds) = $self->sql_maker->update(
        $self->table,
        $args,
        $where,
    );
    my $dbh = $self->dbh;
    return $dbh->do($sql, undef, @binds);
}

sub delete {
    my ($self, $id) = @_;

    $self->cache_delete( $self->table => $id ) if ! ref $id;
    my ($sql, @binds) = $self->sql_maker->delete(
        $self->table,
        ref $id eq 'HASH' ? $id : { id => $id }
    );
    return $self->dbh->do($sql, undef, @binds);
}

sub count {
    my ($self, $where) = @_;
    my ($sql, @binds) = $self->sql_maker->select( $self->table, [ \'COUNT(*)' ], $where );
    my ($count) = $self->dbh->selectrow_array(
        $sql,
        {},
        @binds,
    );
    return $count;
}

no Mouse::Role;

1;
