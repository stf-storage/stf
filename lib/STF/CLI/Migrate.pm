package STF::CLI::Migrate;
use strict;
use parent qw(STF::CLI::Base);
use STF::Migrator;

sub opt_specs {
    (
        'connect_info=s@',
        'concurrency=i',
        'max_object_id=s',
        'min_object_id=s',
        'segment_size=i',
        'storage_id=i',
        'workers=i',
        'help!',
    )
}

sub run {
    my $self = shift;

    my $options = $self->{options};
    if ( $options->{help} ) {
        $self->show_help();
        exit 0;
    }

    my $migrator = STF::Migrator->new($options);
    $migrator->run;
}

sub show_help {
    print <<EOM;
$0 migrate [options]

The migrate command replicates objects from a particular storage to
other available storages. This utility will replicate the object to
ANY available storage, so you must disable storages that you do not
want to have new objects replicated in.

Options:
    connect_info:    Database connect info. Pass it a DSN.
                     (e.g. dbi:mysql:dbname=foo;user=root;password=blah)

                     Alternatively, you may specify this option multiple 
                     times 
                        --connect_info=dbi:...
                        --connect_info=user
                        --connect_info=password

                     This option is required.

    storage_id:      The storage ID in the storage table.

                     This option is required.

    max_object_id:   The object ID to start migrating from.
                     (default: maximum object ID in storage)

    min_object_id:   The object ID to stop migrating at.
                     (default: minimum object ID in storage)

    workers:         The number of workers to spawn (default: 5)

    segment_size:    Number of objects each worker should check
                     (default: 5000)

    concurrency:     Number of concurrent connections each worker
                     should open (default: 10)

EOM
}

1;