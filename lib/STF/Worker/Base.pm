package STF::Worker::Base;
use strict;
use STF::Constants qw(STF_DEBUG);
use Class::Load ();
use Class::Accessor::Lite
    rw => [ qw( loop_class interval max_works_per_child ) ]
;

sub new {
    my ($class, %args) = @_;
    my $self = bless {
        interval => 1,
        max_works_per_child => 1_000,
        %args
    }, $class;
    if (! $self->loop_class) {
        $self->loop_class( 'Periodic' );
    }

    return $self;
}

sub create_loop {
    my $self = shift;
    my $klass = $self->loop_class;

    if ( $klass !~ s/^\+// ) {
        $klass = "STF::Worker::Loop::$klass";
    }
    Class::Load::is_class_loaded($klass) or
        Class::Load::load_class($klass);

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
        print STDERR "[Worker] Starting $self worker...\n";
    }

    my $loop = $self->create_loop;
    if ( STF_DEBUG ) {
        print STDERR "[Worker] Instantiated loop: $loop\n";
    }

    $loop->work( $self );
}

1;

__END__

=head1 NAME

STF::Worker::Base - Base Worker Class

=cut
