use strict;
use Test::More;

my $have_schwartz = eval { require TheSchwartz };
my @modules = map {
    my $f = $_;
    $f =~ s{^lib/}{};
    $f =~ s{\.pm$}{};
    $f =~ s{/}{::}g;
    $f;
} split /\n/, `find lib -name '*.pm'`;

foreach my $module (@modules) {
    SKIP: {
        if ( $module =~ /Schwartz/ && ! $have_schwartz ) {
            skip 1, "TheSchwartz is not available";
        }
        use_ok $module;
    }
}

done_testing;