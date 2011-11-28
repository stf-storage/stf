package STF::Trait::WithExportableState;
use strict;
use YAML ();
use Class::Accessor::Lite
    rw => [ qw(state_file) ]
;

sub save_state {
    my ($self, $state) = @_;
    my $state_file = $self->state_file or
        die "No state file specified!";
    $state->{__TIMESTAMP__} = time();
    YAML::DumpFile( $state_file, $state );
}

sub load_state {
    my ($self) = @_;
    my $state_file = $self->state_file or
        die "No state file specified!";
    if (-f $state_file) {
        return YAML::LoadFile( $state_file );
    } else {
        return {};
    }
}

1;
