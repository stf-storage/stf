package STF::Trait::WithCache;
use strict;
use STF::Constants qw(STF_CACHE_DEBUG);
use parent qw(STF::Trait::WithContainer);
use Class::Accessor::Lite
    rw => [ qw(cache_expires) ]
;

our $DEFAULT_CACHE_EXPIRES = 5 * 60;

sub cache_key {
    my ($self, @keys) = @_;
    join '.', @keys;
}

sub cache_get {
    my ($self, @keys) = @_;
    my $key = $self->cache_key(@keys);
    my $ret = $self->get('Memcached')->get( $key );
    if (STF_CACHE_DEBUG) {
        printf STDERR " + Cache %s for %s\n",
            ( defined $ret ? "HIT" : "MISS" ),
            $key
    }
    return $ret;
}

sub cache_set {
    my ($self, $key, $value, $expires) = @_;
    $key = ref $key eq 'ARRAY' ? $self->cache_key(@$key) : $key;
    if (STF_CACHE_DEBUG) {
        printf STDERR " + Cache SET for %s\n", $key
    }
    $self->get('Memcached')->set( $key, $value, $expires || $self->cache_expires || $DEFAULT_CACHE_EXPIRES );
}

sub cache_delete {
    my ($self, @keys) = @_;
    my $key = $self->cache_key(@keys);
    if (STF_CACHE_DEBUG) {
        printf STDERR " + Cache DELETE for %s\n", $key
    }
    $self->get('Memcached')->delete( $key );
}

1;
