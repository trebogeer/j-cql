j-cql    [![Build Status](https://travis-ci.org/trebogeer/j-cql.svg?branch=master)](https://travis-ci.org/trebogeer/j-cql)
=====

JCQL is a tool allowing to generate boilerplate java code from existing
cassandra schema. It is intended to be used with Cassandra 2.1+ due to support of UDTs (User Defined Types)/Tuples/Collections.
Cassandra's UDTs/Tuples/Collections and an ability to introspect schema through java driver make it possible to automatically
generate POJOs and corresponding mappers between database and java models. Properly generated java code saves development efforts
and is less error-prone compared to hand coding. Accompanied with proper CI and deployment it can also guarantee consistency between
 database and java models at any point of application lifecycle from development to production rollout.


To try out:
=====

1. git clone https://github.com/trebogeer/j-cql.git
2. cd j-cql/jcql
3. mvn clean verify

 Automatically generated java code from sample cassandra schema (../src/main/resources/schema.cql) will become available 
under ../target/autogen/ 

(ツ)
