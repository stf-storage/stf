package STF::Log;
use Log::Minimal ();

our $PREFIX = "";
$Log::Minimal::PRINT = sub {
    my ($time, $type, $message, $trace) = @_;
    printf STDERR "%5s [%s] %s %s\n",
        $$,
        $type,
        $PREFIX ? sprintf "[%10s]", $PREFIX : "",
        $message;
};

sub import {
    my $class = shift;
    Log::Minimal->export_to_level(1, @_);
}

1;
