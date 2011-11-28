use strict;
use Test::More;

my @modules = map {
    my $f = $_;
    $f =~ s{^lib/}{};
    $f =~ s{\.pm$}{};
    $f =~ s{/}{::}g;
    $f;
} split /\n/, `find lib -name '*.pm'`;

use_ok $_ for @modules;

done_testing;