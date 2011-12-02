package STF::Xslate::Utils;
use strict;
use parent qw(Text::Xslate::Bridge);
use STF::Utils ();
use STF::Constants ();

__PACKAGE__->bridge(
    function => {
        error_msgs => Text::Xslate::html_builder(\&error_msgs),
        loc      => sub { $_[0] },
        mode_str => \&mode_str,
        paginate => Text::Xslate::html_builder(\&paginate),
        human_readable => \&STF::Utils::human_readable_size,
    }
);

sub error_msgs {
    my ($result) = @_;

    if (! defined $result) {
        return '';
    }

    if ( $result->success) {
        return '';
    }

    my $msgs = $result->msgs;
    return sprintf '<ul class="error">%s</ul>',
        join '', map { "<li>$_: @{$msgs->{$_}}</li>" }
            keys %$msgs;
}

sub paginate {
    my ($uri, $pager) = @_;

    my $form = $uri->query_form;
    sprintf qq{<div class="pagination">%s | %s</div>},
        $pager->previous_page ?
            sprintf '<a href="%s">Prev</a>',
                do {
                    my $u = $uri->clone;
                    $u->query_form( %$form, p => $pager->previous_page );
                    $u;
                }
            :
            "Prev",
        $pager->next_page ?
            sprintf '<a href="%s">Next</a>',
                do {
                    my $u = $uri->clone;
                    $u->query_form( %$form, p => $pager->next_page );
                    $u;
                }
            :
            "Next"
    ;
}

my %mode_str = (
    STF::Constants::STORAGE_MODE_CRASH_RECOVERED() => 'crash_recovered',
    STF::Constants::STORAGE_MODE_CRASH_RECOVER_NOW() => 'crash_recovering',
    STF::Constants::STORAGE_MODE_CRASH() => 'crash',
    STF::Constants::STORAGE_MODE_RETIRE() => 'retire',
    STF::Constants::STORAGE_MODE_MIGRATE_NOW() => 'migrating',
    STF::Constants::STORAGE_MODE_MIGRATED() => 'migrated',
    STF::Constants::STORAGE_MODE_READ_WRITE() => 'rw',
    STF::Constants::STORAGE_MODE_READ_ONLY() => 'ro',
    STF::Constants::STORAGE_MODE_TEMPORARILY_DOWN() => 'down',
);

sub mode_str {
    return $mode_str{$_[0]} || "unknown ($_[0])";
}

1;