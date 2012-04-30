package STF::CLI::Health;
use strict;
use parent qw(STF::CLI::Base);
use JSON ();
use Digest::MD5 ();

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

    my $storage_api = $self->get('API::Storage');
    my $entity_api  = $self->get('API::Entity');
    my @entities    = $entity_api->search({
        object_id => $object->{id},
    });
    my $md5 = Digest::MD5->new;
    my @results;
    foreach my $entity (@entities) {
        my $storage = $storage_api->lookup($entity->{storage_id});
        my $url     = join "/", $storage->{uri}, $object->{internal_name};

        my $content = $entity_api->fetch_content({
            storage => $storage,
            object  => $object,
        });
        if ($content) {
            $md5->reset;
            $md5->addfile($content);
        }

        push @results, {
            url     => $url,
            storage => $storage,
            $content ? 
                ( valid => JSON::true(), md5 => $md5->hexdigest ) :
                ( valid => JSON::false(), md5 => undef ),
        };
    }

    my $formatter = JSON->new->pretty;
    $formatter->encode({
        object => $object,
        entities => \@results,
    });
    
    print "---\n";
}

1;