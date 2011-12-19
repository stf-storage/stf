package STF::CLI::Health;
use strict;
use parent qw(STF::CLI::Base);
use JSON ();

sub opt_specs {
    (
        'all!',
        'storage=i',
    )
}

sub run {
    my ($self, $object_id) = @_;

    my $options = $self->{options};
    if ( my $storage = $options->{storage} ) {
        $self->show_object_health_in_storage( $storage, $options->{limit} );
    } else {
        my $object = $self->get_object($object_id);
        $self->show_object_health( $object );
    }
}

sub show_object_health_in_storage {
    my ($self, $storage_id, $limit) = @_;
    if ($limit <= 0) {
        $limit = 100;
    }

    my $dbh = $self->get('DB::Master');
    my $sth = $dbh->prepare(<<EOSQL);
        SELECT object.id
            FROM object JOIN bucket ON object.bucket_id = bucket.id
                        JOIN entity ON object.id = entity.object_id
            WHERE entity.storage_id = ? LIMIT ?
EOSQL
    $sth->execute( $storage_id, $limit );
    while ( my $h = $sth->fetchrow_hashref ) {
        my $object = $self->get_object( $h->{id} );
        $self->show_object_health( $object );
    }
}

sub show_object_health {
    my ($self, $object) = @_;

    my $all = $self->{options}->{all};

    my ($valids, $invalids) =
        $self->get('API::Object')->check_health($object->{id});

    my $formatter = JSON->new->pretty;
    if ( $all ) {
        print $formatter->encode({
            object => $object,
            valids => $valids,
            invalids => $invalids,
        });
    } else {
        print $formatter->encode({
            object => $object->{id},
            valids => scalar @$valids,
            invalids => scalar @$invalids
        });
    }
    print "---\n";
}

1;