use strict;
use Test::More;

my $have_sqlite = eval { require DBD::SQLite };
my $have_schwartz = eval { require TheSchwartz };
my $have_resque = eval { require Resque };
my @modules = map {
    my $f = $_;
    $f =~ s{^lib/}{};
    $f =~ s{\.pm$}{};
    $f =~ s{/}{::}g;
    $f;
} split /\n/, `find lib -name '*.pm'`;

foreach my $module (@modules) {
    SKIP: {
        if ( $module =~ /SQLite/ && ! $have_sqlite ) {
            skip "DBD::SQLite is not available", 1;
        }

        if ( $module =~ /Schwartz/ && ! $have_schwartz ) {
            skip "TheSchwartz is not available", 1;
        }

        if ( $module =~ /Resque/ && ! $have_resque ) {
            skip "Resque is not available", 1;
        }

        use_ok $module;
    }
}

done_testing;