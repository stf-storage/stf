package STF::API::Queue;
use Mouse::Role;

has queue_names => (
    is => 'rw',
    required => 1,
);

requires qw(
    enqueue
    size_for_queue
);

sub size_total {
    my ($self, $func) = @_;
    my $total = 0;
    foreach my $queue_name ( @{ $self->queue_names } ) {
        $total += $self->size_for_queue( $func, $queue_name );
    }
    return $total;
}

sub size {
    no warnings 'redefine';
    *size = \&size_total;
    goto \&size;
}

no Mouse::Role;

1;