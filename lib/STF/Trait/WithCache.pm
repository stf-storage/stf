package STF::Trait::WithCache;
use Mouse::Role;
use STF::Constants qw(STF_CACHE_DEBUG);
use STF::Log;

with 'STF::Trait::WithContainer';

our $DEFAULT_CACHE_EXPIRES = 5 * 60;
has cache_expires => (
    is => 'rw',
    default => $DEFAULT_CACHE_EXPIRES
);

sub cache_key {
    my ($self, @keys) = @_;
    my $key = join '.', @keys;
    if (STF_CACHE_DEBUG) {
        debugf("Generated cache key '%s' from [%s]",
            $key,
            join ", ", @keys
        );
    }
    return $key;
}

sub cache_get_multi {
    my ($self, @keys) = @_;

    local $STF::Log::PREFIX = "Cache";
    my $ret = $self->get('Memcached')->get_multi( @keys );
    if (STF_CACHE_DEBUG) {
        debugf("GET MULTI for (%s) returned %d values", join(", ", @keys), keys %$ret);
        foreach my $key (@keys) {
            debugf("   %s -> %s", $key, $ret->{$key} ? "HIT" : "MISS");
        }
    }
    return $ret;
}

sub cache_get {
    my ($self, @keys) = @_;
    local $STF::Log::PREFIX = "Cache";
    my $key = $self->cache_key(@keys);
    my $ret = $self->get('Memcached')->get( $key );
    if (STF_CACHE_DEBUG) {
        debugf("GET %s for %s", ( defined $ret ? "HIT" : "MISS" ), $key);
    }
    return $ret;
}

sub cache_set {
    my ($self, $key, $value, $expires) = @_;
    local $STF::Log::PREFIX = "Cache";
    $key = ref $key eq 'ARRAY' ? $self->cache_key(@$key) : $key;
    if (STF_CACHE_DEBUG) {
        debugf("SET for %s", $key);
    }
    $self->get('Memcached')->set( $key, $value, $expires || $self->cache_expires || $DEFAULT_CACHE_EXPIRES );
}

sub cache_delete {
    my ($self, @keys) = @_;

    local $STF::Log::PREFIX = "Cache";
    my $key = $self->cache_key(@keys);
    if (STF_CACHE_DEBUG) {
        debugf("DELETE for %s", $key);
    }
    $self->get('Memcached')->delete( $key );
}

no Mouse::Role;

1;
