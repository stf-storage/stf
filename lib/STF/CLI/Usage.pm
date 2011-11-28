package STF::CLI::Usage;
use strict;
use parent qw(STF::CLI::Base);

sub opt_specs { ( 'update' ); }

sub run {
    my( $self, $id ) = @_;
    my @storages;
    my $model = $self->get('API::Storage');
    if ( $id ) {
        @storages = ($model->lookup( $id ));
    }
    else {
        @storages = $model->search;
    }
    for my $storage( @storages ) {
        my $size = $storage->{used};
        if ( $self->options->{update} ) {
            $size = $model->update_usage( $storage->{id} );
        }
        printf "storage(%d) %s %s\n", $storage->{id}, $storage->{uri}, human_readable_size($size);
    }
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


1;

__END__
