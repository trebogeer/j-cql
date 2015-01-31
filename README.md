## j-cql    [![Build Status](https://travis-ci.org/trebogeer/j-cql.svg?branch=master)](https://travis-ci.org/trebogeer/j-cql)


JCQL is a tool allowing to generate boilerplate java code from existing
cassandra schema. It is intended to be used with Cassandra 2.1+ due to support of UDTs (User Defined Types)/Tuples/Collections.
Cassandra's UDTs/Tuples/Collections and an ability to introspect schema through java driver make it possible to automatically
generate POJOs and corresponding mappers between database and java models. Properly generated java code saves development efforts
and is less error-prone compared to hand coding. Accompanied with proper CI and deployment it can also guarantee consistency between
database and java models at any point of application lifecycle from development to production rollout. JCQL does not rely on java 
reflection or annotations which means all discrepancies between actual cassandra schema and what client code expects it to be will be identified
during compilation not at runtime in the middle of the night right after production release. No need to worry about Cassandra client code
performance implications due to use of reflection. 

### Key features

 * Lightweight: Depends only on cassandra java driver and tiny javatuples library to support Cassandra tuples.
 
 * POJO-oriented development: Maps Cassandra rows to automatically generated domain objects so you can still work with the Object Oriented aspect of Java.

 * Easy to learn and use: JCQL is extremely simple to use. After code is generated there are only three interfaces you need to worry about to map data from 
 <br/> POJOs to Statements and back.

### To try out

* _git clone https://github.com/trebogeer/j-cql.git_
* _cd j-cql/jcql_
* _mvn clean verify_

Automatically generated java code from sample cassandra schema (../src/main/resources/schema.cql) will become available 
under ../target/autogen/ 

## Documentation

### Project Home
[http://trebogeer.github.io/j-cql](http://trebogeer.github.io/j-cql)

## Current version

The current stable version of JCQL is 0.8.2
<br/>
The current development version of JCQL is 9.0.0-SNAPSHOT
<br/>

## License
JCQL is released under the [Apache License, Version 2.0](http://www.apache.org/licenses/LICENSE-2.0.txt).

(ツ)
