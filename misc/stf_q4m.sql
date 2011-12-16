INSTALL PLUGIN queue SONAME 'libqueue_engine.so';
CREATE FUNCTION queue_wait RETURNS INT SONAME 'libqueue_engine.so';
CREATE FUNCTION queue_end RETURNS INT SONAME 'libqueue_engine.so';
CREATE FUNCTION queue_abort RETURNS INT SONAME 'libqueue_engine.so';
CREATE FUNCTION queue_rowid RETURNS INT SONAME 'libqueue_engine.so';
CREATE FUNCTION queue_set_srcid RETURNS INT SONAME 'libqueue_engine.so';
CREATE FUNCTION queue_compact RETURNS INT SONAME 'libqueue_engine.so';

CREATE TABLE queue_replicate (
    args VARCHAR(255) NOT NULL,
    num_fails INT DEFAULT 0 NOT NULL,
    retry_at INT DEFAULT 0 NOT NULL,
    created_at INT NOT NULL
) ENGINE=QUEUE;

CREATE TABLE queue_delete_bucket (
    args VARCHAR(255) NOT NULL,
    num_fails INT DEFAULT 0 NOT NULL,
    retry_at INT DEFAULT 0 NOT NULL,
    created_at INT NOT NULL
) ENGINE=QUEUE;

CREATE TABLE queue_delete_object (
    args VARCHAR(255) NOT NULL,
    num_fails INT DEFAULT 0 NOT NULL,
    retry_at INT DEFAULT 0 NOT NULL,
    created_at INT NOT NULL
) ENGINE=QUEUE;

CREATE TABLE queue_repair_object (
    args VARCHAR(255) NOT NULL,
    num_fails INT DEFAULT 0 NOT NULL,
    retry_at INT DEFAULT 0 NOT NULL,
    created_at INT NOT NULL
) ENGINE=QUEUE;

CREATE TABLE queue_object_health (
    args VARCHAR(255) NOT NULL,
    num_fails INT DEFAULT 0 NOT NULL,
    retry_at INT DEFAULT 0 NOT NULL,
    created_at INT NOT NULL
) ENGINE=QUEUE;

