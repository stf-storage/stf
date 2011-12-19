package STF;
our $VERSION = '1.00';

1;

__END__

=head1 NAME

STF - Scalable, Simple Distributed Object Storage

=head1 SYNOPSIS

    see http://stf-storage.github.com

=head1 DESCRIPTION

STF is a distributed object storage, built with Perl, MySQL, Q4M (or TheSchwartz), and Memcached.

STF uses HTTP as its protocol, so it's very easy for your applications to talk to it.

=head1 SEE ALSO

http://stf-storage.github.com - project page.

L<STF::Dispatcher::PSGI>

L<Net::STF::Client>

=head1 AUTHOR

Daisuke Maki C<< <daisuke@endeworks.jp> >>

=head1 AUTHOR EMERITUS

Ikebe Tomohiro

=head1 COPYRIGHT AND LICENSE

Copyright (C) 2011 by livedoor, inc.

This library is free software; you can redistribute it and/or modify
it under the The Artistic License 2.0 (GPL Compatible)

L<http://www.opensource.org/licenses/Artistic-2.0>

=cut