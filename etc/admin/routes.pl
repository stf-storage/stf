use Router::Simple::Declare;

router {
    connect '/' => {
        controller => 'Global',
        action     => 'index',
    };

    my %namespace = (
        Storage => 'storage',
        Cluster => 'cluster'
    );

    connect '/cluster/storage/unclustered' => {
        controller => 'Cluster',
        action     => 'storage_unclustered',
    };
    connect '/cluster/storage/change.json' => {
        controller => 'Cluster',
        action     => 'storage_change',
    };

    while ( my ($controller, $namespace) = each %namespace ) {
        connect qr{^/$namespace(?:/(?:list)?)?$} => {
            controller => $controller,
            action     => 'list',
        };

        connect "/$namespace/add" => {
            controller => $controller,
            action     => 'add',
        }, { method => 'GET' };

        connect "/$namespace/add" => {
            controller => $controller,
            action     => 'add_post',
        }, { method => 'POST' };

        connect "/$namespace/:object_id/delete" => {
            controller => $controller,
            action     => 'delete_post',
        }, { method => 'POST' };

        foreach my $action (qw(edit)) {
            connect "/$namespace/:object_id/$action" => {
                controller => $controller,
                action     => $action,
            }, { method => 'GET' };
            connect "/$namespace/:object_id/$action" => {
                controller => $controller,
                action     => "${action}_post",
            }, { method => 'POST' };
        }
    }

    foreach my $action (qw(entities)) {
        connect "/storage/:object_id/$action" => {
            controller => 'Storage',
            action     => $action,
        };
    }

    connect "/bucket/add" => {
        controller => 'Bucket',
        action     => 'add',
    }, { method => 'GET' };
    connect "/bucket/add" => {
        controller => 'Bucket',
        action     => 'add_post',
    }, { method => 'POST' };

    connect qr{^/bucket(?:/(?:list)?)?$} => {
        controller => 'Bucket',
        action     => 'list',
    };

    connect '/bucket/show/:bucket_id' => {
        controller => 'Bucket',
        action     => 'view',
    };

    foreach my $action ( qw(delete) ) {
        connect "/ajax/bucket/:bucket_id/$action.json" => {
            controller => 'Bucket',
            action     => $action,
        }, { method => "POST" };
    }
    foreach my $action ( qw(repair delete) ) {
        connect "/ajax/object/:object_id/$action.json" => {
            controller => 'Object',
            action     => $action,
        }, { method => "POST" };
    }

    connect '/object/create' => {
        controller => 'Object',
        action     => 'create',
    }, { method => "GET" };
    connect '/object/create' => {
        controller => 'Object',
        action     => 'create_post',
    }, { method => "POST" };

    connect qr{^/object/show/([^/]+)/([\w\/%+._-]+)$} => {
        controller => 'Object',
        action => 'view_public_name',
    };

    connect '/object/show/:object_id' => {
        controller => 'Object',
        action     => 'view',
    };
};
