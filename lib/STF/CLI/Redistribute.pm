package STF::CLI::Redistribute;
use strict;
use feature 'state';
use parent qw( STF::CLI::Base );
use DBI;
use Furl::HTTP;
use HTTP::Status;
use SQL::Maker;
use URI;
use STF::Constants qw(ENTITY_ACTIVE);

sub opt_specs {
    (
        'connect-info=s@',
        'start=s', # object id to start
        'limit=i', # number of objects to select
        'exclude-storage=s@',
        'use-storages=s@',
        'replicate=i', # number of replicas to create
        'stf=s',       # stf URL
    );
}

sub dbh {
    my $self = shift;
    return $self->{dbh} ||= DBI->connect( @{ $self->options->{connect_info} } );
}

sub run {
    my ($self) = @_;

    my $options = $self->options;
    foreach my $key ( keys %$options ) {
        my $new = $key;
        if ( $new =~ s/-/_/g ) {
            $options->{$new} = delete $options->{$key};
        }
    }
    $options->{start} ||= 0;
    $options->{limit} ||= 1000;
    $options->{replicate} ||= 3;
    if (! $options->{stf} ) {
        die "--stf must be specified";
    }

    my $dbh = $self->dbh;

    # replication count on the object table is IGNORED

    # start at given object_id, process up to $limit objects
    my $select_obj = $dbh->prepare( <<EOSQL );
        SELECT o.id, o.name, o.internal_name, o.created_at, b.name FROM object o JOIN bucket b ON
            b.id = o.bucket_id AND
            o.status = 1
            WHERE o.id > ? ORDER BY id ASC LIMIT ?
EOSQL
    my $create_entity = $dbh->prepare( <<EOSQL );
        INSERT
            INTO entity (object_id, storage_id, status, created_at)
            VALUES (?, ?, ?, UNIX_TIMESTAMP(NOW()))
EOSQL

    my ( $object_id, $object_name, $internal_name, $created_at, $bucket_name );
    $select_obj->execute( $options->{start}, $options->{limit} );
    $select_obj->bind_columns( \( $object_id, $object_name, $internal_name, $created_at, $bucket_name ) );

    my $processed = 0;
    my $furl = Furl::HTTP->new;
ITERATE_OBJECTS:
    while ( $select_obj->fetchrow_arrayref ) {
        my $url = URI->new( "$options->{stf}/$bucket_name/$object_name" )->canonical;

        print "$object_id: START (url = $url)\n";
        my ($code, $body, $get_ok);

        for my $try ( 1..10 ) {
            (undef, $code, undef, undef, $body) = $furl->get( $url );
            if ( $get_ok = HTTP::Status::is_success( $code ) ) {
                last;
            }

            print "$object_id: FAIL #$try: Could not retrieve $url ($code)\n";
        }
        if (! $get_ok ) {
            print "$object_id: BROKEN All requests to $url failed\n";
            next;
        }

        my $copied = 0;
        # select storages
        my $storages = $self->select_storages( $object_id );
        foreach my $storage ( @$storages ) {
            my $entity_url = URI->new( "$storage->{uri}/$internal_name" )->canonical;
            my (undef, $e_code) = $furl->put( $entity_url, [ 'X-STF-Object-Timestamp' => $created_at ], $body );
            print "$object_id: PUT $entity_url (storage = $storage->{id})\n";
            if ( HTTP::Status::is_success( $e_code ) ) {
                $create_entity->execute( $object_id, $storage->{id}, ENTITY_ACTIVE );
                $copied++;
            }
        }
        print "$object_id: COPIED $copied\n";
        $processed++;
    }

    print "PROCESSED $processed objects\n";
}

# select storages based on rules.
sub select_storages {
    my ( $self, $object_id ) = @_;

    # $object_id is only used if exclude-storages and use-storages are not
    # specified by the user

    my ($sth, @binds);
    my $dbh = $self->dbh;
    my $options = $self->options;
    if ( my $list = $options->{use_storages} ) {
        my $sql_maker = SQL::Maker::Select->new();
        $sql_maker
            ->add_select( 'id' )
            ->add_select( 'uri' )
            ->add_from( 'storage' )
            ->add_where( mode => 1 )
            ->add_where( id => { IN => $list } )
            ->add_order_by(\'rand()')
            ->limit( $options->{replicate} )
        ;

        $sth = $dbh->prepare( $sql_maker->as_sql );
        @binds = $sql_maker->bind;
    } elsif ( my $list = $options->{exclude_storages} ) {
        my $sql_maker = SQL::Maker::Select->new();
        $sql_maker
            ->add_select( 'storage_id' )
            ->add_from( 'entity' )
            ->add_where( 'object_id' => $object_id )
            ->add_where( 'entity.id' => { 'NOT IN' => $list } )
        ;
        my $combined = sprintf( <<EOSQL, $sql_maker->as_sql );
            SELECT s.id, s.uri FROM storage s
                WHERE s.mode = 1 AND s.id NOT IN ( %s )
            ORDER BY rand() LIMIT $options->{replicate}
EOSQL

        $sth = $dbh->prepare( $combined );
        @binds = $sql_maker->binds;
    } else {
        $sth = $dbh->prepare( <<EOSQL );
            SELECT s.id, s.uri FROM storage s
                WHERE s.mode = 1 AND s.id NOT IN
                (SELECT storage_id FROM entity WHERE object_id = ?)
            ORDER BY rand() LIMIT $options->{replicate}
EOSQL
        @binds = ($object_id);
    }

    $sth->execute( @binds );
    return $sth->fetchall_arrayref({});
}

1;