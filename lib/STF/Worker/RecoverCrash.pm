package STF::Worker::RecoverCrash;
use strict;
use parent qw(STF::Worker::Base STF::Trait::WithContainer);
use STF::Constants qw(:storage STF_DEBUG);
use STF::Utils ();

sub new {
    my $class = shift;
    $class->SUPER::new(
        interval => 5 * 60,
        @_,
        loop_class => "Periodic"
    );
}

sub work_once {
    my $self = shift;

    eval {
        my $api = $self->get('API::Storage');
        my @storages = $api->search( { mode => STORAGE_MODE_CRASH  } );

        foreach my $storage ( @storages ) {
            my $storage_id = $storage->{id};
            if ( STF_DEBUG ) {
                printf STDERR "[     Crash] Recovering storage %s\n",
                    $storage_id
                ;
            }
            $api->update( $storage_id, { mode => STORAGE_MODE_CRASH_RECOVER_NOW } );
            my $guard = Guard::guard {
                STF::Utils::timeout_call( 2, sub {
                    eval {
                        $api->update( $storage_id, { mode => STORAGE_MODE_CRASH } );
                    };
                } );
            };

            # Signals terminate the process, but don't allow us to fire the
            # guard object, so we manually fire it up
            my $sig   = sub {
                my $sig = shift;
                return sub {
                    undef $guard;
                    die "Received signal $sig, bailing out";
                };
            };
            local $SIG{INT}  = $sig->("INT");
            local $SIG{QUIT} = $sig->("QUIT");
            local $SIG{TERM} = $sig->("TERM");
            my $processed = $api->move_entities( $storage_id );

            $guard->cancel;
            if (STF_DEBUG) {
                printf STDERR "[     Crash] Storage %d, processed %d rows\n",
                    $storage_id, $processed;
            }
            $api->update( $storage_id => { mode => STORAGE_MODE_CRASH_RECOVERED } );
        }

        if (STF_DEBUG) {
            printf STDERR "[     Crash] Recovered %d storages\n",
                scalar @storages;
        }
    };
    if ($@) {
        Carp::confess("Failed to run recover crash: $@");
    }
}

1;