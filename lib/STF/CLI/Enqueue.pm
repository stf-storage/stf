package STF::CLI::Enqueue;
use strict;
use parent qw(STF::CLI::Base);

sub run {
    my ($self, $job_name, $arg) = @_;
    $self->get('API::Queue')->enqueue( $job_name, $arg );
    print "Enqueued $arg to $job_name\n";
}

1;
