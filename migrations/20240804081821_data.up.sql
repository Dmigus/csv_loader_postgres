CREATE TABLE IF NOT EXISTS dataset (
    dt date PRIMARY KEY,
    open NUMERIC(10, 2) NOT NULL,
    high NUMERIC(10, 2) NOT NULL,
    low NUMERIC(10, 2) NOT NULL,
    close NUMERIC(10, 2) NOT NULL,
    volume bigint not null
);

