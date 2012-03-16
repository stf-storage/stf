package STF::API::StorageMeta;
use Mouse;

with qw( STF::API::WithDBI );

sub update_for {
    my ($self, $storage_id, $args) = @_;
    $self->create(
        { %$args, storage_id => $storage_id },
        { prefix => "REPLACE INTO" }
    );
}

no Mouse;

1;

