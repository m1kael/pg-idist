
-- drop entire schema, which will cascade through functions too
DROP SCHEMA if exists idist CASCADE;
CREATE SCHEMA idist;

--points only to our schema so we don't have to qualify everything with it
SET search_path TO idist;

-- old: drop table if exists info, data, refs, index cascade;

CREATE TABLE IF NOT EXISTS info (
    id  serial,
    key  varchar(255) unique,
    val  varchar(255)
);

CREATE TABLE IF NOT EXISTS data (
    id  int,
    dims real[],
    pid int         -- partition id
);

CREATE TABLE IF NOT EXISTS refs (
    id  int,
    dims  real[],
    distmax real,   -- distance of furthest assigned point
    distmaxid int
);

CREATE TABLE IF NOT EXISTS index (
    id  int,
    val real
);


-- hide context messages which state where notice messages came from
-- instead of DEFAULT or VERBOSE, use TERSE
-- must be it's own line to work in this script
\set VERBOSITY 'terse'  

