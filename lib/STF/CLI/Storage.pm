package STF::CLI::Storage;
use strict;
use parent qw(STF::CLI::Base);

sub opt_specs {
    (
        'list|L!',
        'limit=i',
    )
}

sub run {
    my ($self, $storage_id) = @_;

    my $options = $self->{options};
    if ( $options->{list} ) {
        $self->show_all_storages( $options->{limit} );
    } else {
        my $storage = $self->get_storage( $storage_id );
        if (! $storage ) {
            die "Could not find object '$storage_id'";
        }
        $self->show_storage( $storage );
    }
}

sub show_all_storages {
    my ($self, $limit) = @_;

    if ($limit <= 0) {
        $limit = 100;
    }

    my $dbh = $self->get('DB::Master');
    my $sth = $dbh->prepare(<<EOSQL);
        SELECT * FROM storage LIMIT $limit
EOSQL
    $sth->execute();
    while ( my $h = $sth->fetchrow_hashref ) {
        $self->show_storage( $h );
    }
}
    
sub show_storage {
    my ($self, $storage) = @_;

    my $formatter = JSON->new->pretty;

    local $storage->{created_at} = $self->format_time( $storage->{created_at} );
    local $storage->{updated_at} = $self->format_time( $storage->{updated_at} );
    print $formatter->encode($storage);
    print "---\n";
}

1;

