package STF::Exception::HTTP;
use strict;
use Carp ();

sub throw {
    Carp::croak( bless $_[1], $_[0] );
}

sub as_psgi {
    return [ @{$_[0]} ];
}

1;
