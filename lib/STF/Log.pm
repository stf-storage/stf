package STF::Log;
use Log::Minimal ();

our ($PREFIX, $LOGFH);

BEGIN {
    $PREFIX = "";
    if (my $file = $ENV{STF_LOG_FILE}) {
        open my $fh, '>>', $file
            or die "Could not open log file $file: $!";
        $LOGFH = $fh;
    } else {
        $LOGFH  =\*STDERR;
    }
}

$Log::Minimal::PRINT = sub {
    my ($time, $type, $message, $trace) = @_;
    printf $LOGFH ( "%5s [%s] %s %s\n",
        $$,
        $type,
        $PREFIX ? sprintf "[%10s]", $PREFIX : "",
        $message
    );
};

sub import {
    my $class = shift;
    Log::Minimal->export_to_level(1, @_);
}

1;
