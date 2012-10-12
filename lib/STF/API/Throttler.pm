package STF::API::Throttler;
use Mouse;

with 'STF::Trait::WithContainer';

has key => (
    is => 'ro',
    required => 1,
);

has throttle_span => (
    is => 'ro',
    default => 10
);

has threshold => (
    is => 'rw',
    default => 0
);

sub incr {
    my ($self, $now) = @_;

    $now ||= time();
    my $time = int($now);

    my $key = join ".", $self->key, $time;
    my $memd = $self->get('Memcached');
    if (! $memd->incr($key)) {
        # try initializing once
        if (! $memd->add($key, 1, $self->throttle_span * 2)) {
            # failed? somebody got to the key before us, so
            # try again.
            $memd->incr($key);
        }
    }
}

sub expand_key {
#    my ($key, $base_t, $span) = @_;
    # for max efficiency...
    return map {
        join ".", $_[0], ($_[1] - $_)
    } 0 .. $_[2];
}

sub current_count_multi {
    my ($self, $now, @keys) = @_;

    my $memd = $self->get('Memcached');
    my $time = int($now);
    my $span = $self->throttle_span;
    my %ret;
    foreach my $key (@keys) {
        my $h = $memd->get_multi(
            expand_key($key, $time, $span)
        );

        my $count = 0;
        foreach my $value (values %$h) {
            $count += $value || 0;
        }
        $ret{$key} = $count;
    }

    return \%ret;
}
    
sub current_count {
    my ($self, $now) = @_;

    my $time = int($now);
    my $h = $self->get('Memcached')->get_multi(
        expand_key($self->key, $time, $self->throttle_span)
    );

    my $count = 0;
    foreach my $value (values %$h) {
        $count += $value || 0;
    }
    return $count;
}

sub should_throttle {
    my ($self, $now) = @_;

    my $threshold = $self->threshold;
    my $current   = $self->current_count($now);
    return $threshold <= $current;
}

1;
