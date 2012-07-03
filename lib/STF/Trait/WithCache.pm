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
    join '.', @keys;
}

sub cache_get_multi {
    my ($self, @keys) = @_;
    my $ret = $self->get('Memcached')->get_multi( @keys );
    return $ret;
}

sub cache_get {
    my ($self, @keys) = @_;
    my $key = $self->cache_key(@keys);
    my $ret = $self->get('Memcached')->get( $key );
    if (STF_CACHE_DEBUG) {
        debugf("Cache %s for %s", ( defined $ret ? "HIT" : "MISS" ), $key);
    }
    return $ret;
}

sub cache_set {
    my ($self, $key, $value, $expires) = @_;
    $key = ref $key eq 'ARRAY' ? $self->cache_key(@$key) : $key;
    if (STF_CACHE_DEBUG) {
        debugf("Cache SET for %s", $key);
    }
    $self->get('Memcached')->set( $key, $value, $expires || $self->cache_expires || $DEFAULT_CACHE_EXPIRES );
}

sub cache_delete {
    my ($self, @keys) = @_;
    my $key = $self->cache_key(@keys);
    if (STF_CACHE_DEBUG) {
        debugf("Cache DELETE for %s", $key);
    }
    $self->get('Memcached')->delete( $key );
}

no Mouse::Role;

1;
