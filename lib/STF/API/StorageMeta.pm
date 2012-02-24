package STF::API::StorageMeta;
use strict;
use parent qw( STF::API::WithDBI );
use Class::Accessor::Lite new => 1;

sub create {
    my ($self, $args, $opts) = @_;
    $opts ||= {};
    $opts->{prefix} = "REPLACE INTO";
use Data::Dumper::Concise;
warn Dumper($args);
    $self->SUPER::create($args, $opts);
}

sub update {
    my ($self, $id, $args, $opts) = @_;
    $opts ||= {};
    $opts->{prefix} = "REPLACE INTO";
    $args->{storage_id} = $id;
    $self->SUPER::create($args, $opts);
}

1;

