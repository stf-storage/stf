package STF::API::Throttler;
use Mouse;

with 'STF::Trait::WithContainer';

has key => (
    is => 'ro',
    required => 1,
);

has normalize_base => (
    is => 'ro',
    default => 10
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

    # normalize time to the previous N seconds
    $time -= $time % $self->normalize_base;
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

sub to_base_time {
    my ($self, $now) = @_;
    $now ||= time();
    my $time = int($now);
    $time -= $time % $self->normalize_base;
    return $time;
}

sub expand_key {
#    my ($key, $base_t, $normalize_base, $span) = @_;
    # for max efficiency...
    return map {
        join ".", $_[0], ($_[1] - $_ * $_[2])
    } 0 .. int($_[3] / $_[2])
}

sub current_count_multi {
    my ($self, $now, @keys) = @_;

    my $memd = $self->get('Memcached');
    my $time = $self->to_base_time($now);
    my $normalize_base = $self->normalize_base;
    my $span = $self->throttle_span;
    my %ret;
    foreach my $key (@keys) {
        my $h = $memd->get_multi(
            expand_key($key, $time, $normalize_base, $span)
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

    my $time = $self->to_base_time($now);
    my $h = $self->get('Memcached')->get_multi(
        expand_key($self->key, $time, $self->normalize_base, $self->throttle_span)
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
    return $threshold < $current;
}

1;
