


CREATE TABLE IF NOT EXISTS info (
    id  serial,
    key  varchar(255),
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

