package STF::Worker::Base;
use Mouse;
use STF::Constants qw(STF_DEBUG);

has interval => (
    is => 'rw',
    default => 1_000_000
);

has loop_class => (
    is => 'rw',
    default => 'Periodic',
);

has max_works_per_child => (
    is => 'rw',
    default => 1_000
);

sub create_loop {
    my $self = shift;
    my $klass = $self->loop_class;

    if ( $klass !~ s/^\+// ) {
        $klass = "STF::Worker::Loop::$klass";
    }
    Mouse::Util::is_class_loaded($klass) or
        Mouse::Util::load_class($klass);

    my $loop = $klass->new(
        container => $self->container,
        interval => $self->interval,
        max_works_per_child => $self->max_works_per_child,
    );
    return $loop;
}

sub work {
    my $self = shift;
    if ( STF_DEBUG ) {
        print STDERR "[    Worker] Starting $self worker...\n";
    }

    my $loop = $self->create_loop;
    if ( STF_DEBUG ) {
        print STDERR "[    Worker] Instantiated loop: $loop\n";
    }

    $loop->work( $self );
}

no Mouse;

1;

__END__

=head1 NAME

STF::Worker::Base - Base Worker Class

=cut
