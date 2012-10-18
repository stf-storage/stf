package STF::API::Notification;
use Mouse;

with 'STF::API::WithDBI';

around create => sub {
    my ($next, $self, $args) = @_;

    $args->{created_at} ||= time();
    $args->{severity}   ||= 'info';
    if (! $args->{source}) {
        my $i = 0;
        my @caller = caller($i++);
        while (@caller && $caller[0] !~ /^STF::/) {
            @caller = caller($i++);
        }
        $args->{source} = join ":", @caller ? @caller[1,2] : "(unknown)";
    }
    return unless $self->$next($args);
    my $object = $self->lookup($self->dbh->{mysql_insertid});
    $self->get('API::Queue')->enqueue(notify => $object->{id});
    return $object;
};

no Mouse;

1;