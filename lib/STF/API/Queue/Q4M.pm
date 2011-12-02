package STF::API::Queue::Q4M;
use strict;
use parent qw(STF::Trait::WithDBI);
use STF::Constants qw(:func STF_DEBUG);
use Class::Accessor::Lite
    new => 1,
    rw => [ qw(
        funcmap
    ) ]
;

sub get_func_id {
    my ($self, $func) = @_;

    my $funcmap = $self->funcmap;
    if (! $funcmap) {
        $self->funcmap( $funcmap = {
            replicate     => Q4M_FUNC_REPLICATE,
            delete_object => Q4M_FUNC_DELETE_OBJECT,
            delete_bucket => Q4M_FUNC_DELETE_BUCKET,
            repair_object => Q4M_FUNC_REPAIR_OBJECT,
        } );
    }

    $funcmap->{$func};
}

sub enqueue {
    my ($self, $func, $object_id) = @_;
    my $func_id = $self->get_func_id( $func );
    if (! $func_id ) {
        Carp::confess( "PANIC: Don't know what the function ID for $func is" );
    }

    if ( ! defined $object_id ) {
        Carp::confess("No object_id given for $func");
    }

    my $dbh = $self->dbh('DB::Queue');
    my $table = "queue_$func";

    if (STF_DEBUG) {
        printf STDERR "[     Queue] INSERT %s into %s for %s\n",
            $object_id, $table, $func
        ;
    }
    my $rv  = $dbh->do(<<EOSQL, undef, $object_id );
        INSERT INTO $table ( args, created_at ) VALUES (?, UNIX_TIMESTAMP( NOW() ) )
EOSQL
    return $rv;
}

1;
