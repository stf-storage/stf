package STF::Log;
use Log::Minimal ();
use base qw(Exporter);
our @EXPORT = @Log::Miimal::EXPORT;

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

1;
