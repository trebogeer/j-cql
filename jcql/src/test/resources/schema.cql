
DROP KEYSPACE IF EXISTS jcql;
CREATE KEYSPACE IF NOT EXISTS jcql WITH REPLICATION = {'class':'SimpleStrategy', 'replication_factor':1};

CREATE TYPE jcql.phone (
   alias text,
   number text
);

CREATE TYPE jcql.address (
   street text,
   city text,
   zipCode int,
   tupletest frozen<tuple<phone, text>>,
   map1test map<frozen <phone>, text>,
   map2test map<int, text>,
   map3test map<int, frozen <phone>>,
   map4test map<frozen <phone>, frozen <phone>>,
   list1test list<int>,
   set1test set<text>,
   set2test set<frozen <phone>>,
   primary_phone frozen<phone>,
   phones list<frozen <phone>>
);

CREATE TABLE jcql.accounts (
   email text PRIMARY KEY,
   name text,
   solr_query text,
   addr frozen<address>,
   settest set<text>,
   set2test set<frozen <address>>,
   maptest map<int, text>,
   map2test map<int, frozen <phone>>
);

CREATE TABLE jcql.tuple_test (
   the_key int PRIMARY KEY,
   the1_tuple frozen<tuple<text>>,
   the2_tuple frozen<tuple<int, list<text>>>,
   the3_tuple frozen<tuple<int, text, float>>,
   the4_tuple frozen<tuple<int, phone, float, text>>,
   the5_tuple frozen<tuple<int, float, double, varint, timestamp>>,
   the6_tuple frozen<tuple<int, float, double, varint, tuple<int, text>, text>>
);

CREATE TABLE jcql.simple (
    key text PRIMARY KEY,
    value text,
);
