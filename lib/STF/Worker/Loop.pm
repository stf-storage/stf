package STF::Worker::Loop;
use strict;
use Class::Accessor::Lite
    rw => [ qw(processed max_works_per_child) ]
;

sub new {
    my ($class, %args) = @_;
    return bless {
        max_works_per_child => 1_000,
        %args,
        processed => 0,
    }, $class;
}

sub incr_processed {
    my $self = shift;
    ++$self->{processed};
}

sub should_loop {
    my $self = shift;
    return $self->{processed} < $self->max_works_per_child;
}

1;
