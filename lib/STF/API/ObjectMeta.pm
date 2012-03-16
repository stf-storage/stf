package STF::API::ObjectMeta;
use Mouse;
use Digest::MD5 ();

with 'STF::API::WithDBI';

sub lookup_for {
    my ($self, $object_id) = @_;
    my ($meta) = $self->search( { object_id => $object_id } );
    return $meta;
}

sub update_for {
    my ($self, $object_id, $args) = @_;
    $self->create(
        { %$args, object_id => $object_id },
        { prefix => "REPLACE INTO" }
    );
}

no Mouse;

1;
