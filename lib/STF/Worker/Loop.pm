package STF::Worker::Loop;
use strict;
use Class::Accessor::Lite
    rw => [ qw(interval processed max_works_per_child) ]
;

sub new {
    my ($class, %args) = @_;
    my $self = bless {
        interval => 1,
        max_works_per_child => 1_000,
        %args,
        processed => 0,
    }, $class;
    return $self;
}

sub incr_processed {
    my $self = shift;
    ++$self->{processed};
}

sub should_loop {
    my $self = shift;
    return $self->{processed} < $self->max_works_per_child;
}

sub work {}

1;
