package STF::Utils;
use strict;
use POSIX ':signal_h';
use Time::HiRes ();
use Scope::Guard ();
use STF::Log;

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
    require Time::HiRes;
    my $t0 = [ Time::HiRes::gettimeofday() ];
    return Scope::Guard->new(sub {
        my $elapsed = Time::HiRes::tv_interval($t0);
        undef $t0;
        local $STF::Log::PREFIX = "TIMER";
        debugf("%s took %0.6f seconds", $sub, $elapsed);
    } );
}

# This is a rather forceful timeout wrapper that allows us to, for example,
# wrap calls to things blocking in the C layer (such as some DBD's).
# Returns the error that occurred. If the call timed out, then this
# error is set to "timeout_call timed out (%d secs)"
sub timeout_call {
    my ($timeout, $cb, $timeout_cb, @args) = @_;

    $timeout_cb ||= sub { die sprintf "timeout_call timed out (%d secs)\n", $timeout };

    # signals to mask in the handler
    my $mask = POSIX::SigSet->new( SIGALRM );
    # the handler code ref
    my $action = POSIX::SigAction->new(
        $timeout_cb,
        $mask,
        # not using (perl 5.8.2 and later) 'safe' switch or sa_flags
    );
    my $oldaction = POSIX::SigAction->new();
    sigaction( SIGALRM, $action, $oldaction );
    my $rv;
    eval {
        eval {
            Time::HiRes::alarm($timeout); # seconds before time out
            $cb->(@args);
        };
        Time::HiRes::alarm(0); # cancel alarm (if connect worked fast)
        die "$@\n" if $@; # connect died
    };
    my $e = $@;
    sigaction( SIGALRM, $oldaction );  # restore original signal handler

    return $e;
}

1;
