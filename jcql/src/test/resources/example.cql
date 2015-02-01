
CREATE KEYSPACE IF NOT EXISTS jcql WITH REPLICATION = {'class':'SimpleStrategy', 'replication_factor':1};

CREATE TYPE jcql.bass (
   make text,
   model text
);

CREATE TABLE jcql.bass_player (
   first_name text,
   last_name text,
   year_of_birth timestamp,
   bass_guitars_used set<frozen <bass>>,
   bands list<text>,
   albums map<text, timestamp>
);
