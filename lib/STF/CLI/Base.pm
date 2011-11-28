package STF::CLI::Base;
use strict;
use Class::Accessor::Lite (
    new => 1,
    rw => [qw(context options)]
);

sub opt_specs { (); }
sub get { shift->context->get(@_) }

1;
