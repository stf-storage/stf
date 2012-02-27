package STF::API::ObjectMeta;
use strict;
use parent qw( STF::API::WithDBI );
use Class::Accessor::Lite new => 1;
use Digest::MD5 ();

sub lookup_for {
    my ($self, $object_id) = @_;
    my ($meta) = $self->search( { object_id => $object_id } );
    return $meta;
}

sub update_for {
    my ($self, $object_id, $args) = @_;
    $self->SUPER::create(
        { %$args, object_id => $object_id },
        { prefix => "REPLACE INTO" }
    );
}

1;
