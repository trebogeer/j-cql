
DROP KEYSPACE IF EXISTS jcql;
CREATE KEYSPACE IF NOT EXISTS jcql WITH REPLICATION = {'class':'NetworkTopologyStrategy'};

CREATE TYPE jcql.phone (
   alias text,
   number text);

CREATE TYPE jcql.address (
   street text,
   city text,
   zipCode int,
   phones list<frozen <phone>>
);

CREATE TABLE jcql.accounts (
   email text PRIMARY KEY,
   name text,
   addr frozen<address>,
   settest set<text>,
   maptest map<int, text>
);

CREATE TABLE jcql.tuple_test (
   the_key int PRIMARY KEY,
   the_tuple frozen<tuple<int, text, float>>
);