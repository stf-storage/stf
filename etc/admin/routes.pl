use Router::Simple::Declare;

router {
    connect '/' => {
        controller => 'Storage',
        action     => 'list',
    };

    my %namespace = (
        Storage => 'storage',
        Cluster => 'cluster'
    );

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
            controller => $namespace,
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
        connect "/storage/:storage_id/$action" => {
            controller => 'Storage',
            action     => $action,
        };
    }

    connect qr{^/bucket(?:/(?:list)?)?$} => {
        controller => 'Bucket',
        action     => 'list',
    };

    connect '/bucket/:bucket_id' => {
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

    connect qr{^/object/([^/]+)/([\w\/%+._-]+)$} => {
        controller => 'Object',
        action => 'view_public_name',
    };

    connect '/object/:object_id' => {
        controller => 'Object',
        action     => 'view',
    };
};
