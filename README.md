j-cql    [![Build Status](https://travis-ci.org/trebogeer/j-cql.svg?branch=master)](https://travis-ci.org/trebogeer/j-cql)
=====

JCQL is a tool allowing to generate boilerplate java code from existing
cassandra schema. It is intended to be used with Cassandra 2.1+ due to support of UDTs (User Defined Types)/Tuples/Collections.
Cassandra's UDTs/Tuples/Collections and an ability to introspect schema through java driver make it possible to automatically
generate POJOs and corresponding mappers between database and java models. Properly generated java code saves development efforts
and is less error-prone compared to hand coding. Accompanied with proper CI and deployment it can also guarantee consistency between
database and java models at any point of application lifecycle from development to production rollout. JCQL does not rely on java 
reflection or annotations which means all discrepancies between actual cassandra schema and what client code expects it to be will be identified
during compilation not at runtime in the middle of the night right after production release. No need to worry about Cassandra client code
performance implications due to use of reflection. 


####To try out:

* _git clone https://github.com/trebogeer/j-cql.git_
* _cd j-cql/jcql_
* _mvn clean verify_

Automatically generated java code from sample cassandra schema (../src/main/resources/schema.cql) will become available 
under ../target/autogen/ 

(ツ)
