package STF::AdminWeb::Controller::Bucket;
use strict;
use parent qw(STF::AdminWeb::Controller);

sub objects {
    my ($self, $c) = @_;
    my $num = $c->get('API::Object')->count({
        bucket_id => $c->match->{bucket_id},
    });

    my $res = $c->response;
    $res->content_type('text/plain');
    $res->body( $num );
    $c->finished(1);
}

sub list {
    my ($sef, $c) = @_;
    my $limit = 100;
    my $pager = $c->pager($limit);

    my %q;
    my $req = $c->request;
    if ( my $name = $req->param('name') ) {
        $q{name} = { LIKE => $name };
    }

    my @buckets = $c->get('API::Bucket')->search(
        \%q,
        {
            limit    => $pager->entries_per_page + 1,
            offset   => $pager->skipped,
            order_by => { 'name' => 'ASC' },
        }
    );
    # fool pager
    if ( scalar @buckets > $limit ) {
        $pager->total_entries( $limit * $pager->current_page + 1 );
    }

    my $stash = $c->stash;
    $stash->{pager} = $pager;
    $stash->{buckets} = \@buckets;
    $stash->{fdat} = $req->parameters->as_hashref;
}

sub view {
    my ($self, $c) = @_;

    my $bucket_id = $c->match->{bucket_id};
    my $bucket = $c->get('API::Bucket')->lookup( $bucket_id );
    my $total = $c->get('API::Object')->count({ bucket_id => $bucket_id });
    my $limit = 100;
    my $pager = $c->pager( $limit );

    my @objects = $c->get('API::Object')->search_with_entity_info(
        { bucket_id => $bucket_id },
        {
            limit => $pager->entries_per_page + 1,
            offset => $pager->skipped,
            order_by => { 'name' => 'ASC' },
        }
    );

    if ( scalar @objects > $limit ) {
        $pager->total_entries( $limit * $pager->current_page + 1 );
    }
    my $stash = $c->stash;
    $stash->{bucket} = $bucket;
    $stash->{objects} = \@objects;
    $stash->{pager} = $pager;
}


1;