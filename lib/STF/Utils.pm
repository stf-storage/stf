package STF::Utils;
use strict;
use Guard ();

sub merge_hashes {
    my ($left, $right) = @_;
    return { %$left, %$right };
}

sub applyenv {
    my ($file) = @_;

    my $env = $ENV{DEPLOY_ENV};
    if (! $env ) {
        return ($file);
    }

    my $x_file = $file;
    $x_file =~ s/\.([^\.]+)$/_$env.$1/;
    return ($file, $x_file);
}

sub human_readable_size {
    my $bytes = shift;
    my $val = $bytes; 
    my $unit = 'B';
    for my $u(qw(K M G T)) {
        last if $val < 1024;
        $val /= 1024;
        $unit = $u;
    } 
    return sprintf '%.1f%s', $val, $unit;
}   
    
sub as_bytes {
    my $v = shift;
    if ($v =~ s/TB?$//i) {
        return $v * 1024 * 1024 * 1024 * 1024;
    } elsif ($v =~ s/GB?$//i) {
        return $v * 1024 * 1024 * 1024;
    } elsif ($v =~ s/MB?$//i) {
        return $v * 1024 * 1024;
    } elsif ($v =~ s/KB?$//i) {
        return $v * 1024;
    }  
    return $v; 
}   

sub timer_guard {
    my $sub = $_[0] || (caller(1))[0,3];
    my $t0 = [ Time::HiRes::gettimeofday() ];
    return Guard::guard {
        my $elapsed = Time::HiRes::tv_interval($t0);
        undef $t0;
        printf STDERR "[     TIMER] (%05d) %s took %0.6f seconds\n",
            $$,
            $sub,
            $elapsed
        ;
    };
}

1;
