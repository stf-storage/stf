package STF::API::ObjectMeta;
use strict;
use parent qw( STF::API::WithDBI );
use Class::Accessor::Lite new => 1;
use Digest::MD5 ();

sub update_for {
    my ($self, $storage_id, $args) = @_;
    $self->SUPER::create(
        { %$args, storage_id => $storage_id },
        { prefix => "REPLACE INTO" }
    );
}

1;
