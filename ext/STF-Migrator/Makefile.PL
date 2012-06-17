use inc::Module::Install;

name 'STF-Migrator';
all_from 'lib/STF/Migrator.pm';

requires 'Mouse';
requires 'Coro';
requires 'Fcntl';
requires 'FurlX::Coro';
requires 'Guard';
requires 'HTTP::Status;
requires 'DBIx::Connector';
requires 'Parallel::ForkManager';
requires 'POSIX';

WriteAll;