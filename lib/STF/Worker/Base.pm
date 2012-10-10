package STF::Worker::Base;
use Mouse;
use STF::Constants qw(STF_DEBUG);
use STF::Log;

has name => (
    is => 'rw',
    default => sub {
        my $klass = Scalar::Util::blessed($_[0]);
        $klass =~ s/^STF::Worker:://;
        return $klass;
    }
);

has interval => (
    is => 'rw',
    default => 1_000_000
);

has loop_class => (
    is => 'rw',
    default => 'Periodic',
);

has max_jobs_per_minute => (
    is => 'rw',
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
        max_jobs_per_minute => $self->max_jobs_per_minute,
        counter_key => "stf.worker." . $self->name . ".processed_jobs",
    );
    return $loop;
}

sub work {
    my $self = shift;

    local $STF::Log::PREFIX = $self->name;
    infof("Starting %s worker...", $self);

    my $loop = $self->create_loop;
    debugf("Starting %s worker...", $self) if STF_DEBUG;

    $loop->work( $self );
}

no Mouse;

1;

__END__

=head1 NAME

STF::Worker::Base - Base Worker Class

=cut
