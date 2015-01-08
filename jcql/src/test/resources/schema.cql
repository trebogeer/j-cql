
DROP KEYSPACE IF EXISTS test_ks;
CREATE KEYSPACE IF NOT EXISTS test_ks WITH REPLICATION = {'class':'NetworkTopologyStrategy'};

CREATE TYPE test_ks.phone (
   alias text,
   number text);

CREATE TYPE test_ks.address (
   street text,
   city text,
   zipCode int,
   phones list<frozen <phone>>
);

CREATE TABLE test_ks.accounts (
   email text PRIMARY KEY,
   name text,
   addr frozen<address>
);

CREATE TABLE test_ks.tuple_test (
   the_key int PRIMARY KEY,
   the_tuple frozen<tuple<int, text, float>>
);