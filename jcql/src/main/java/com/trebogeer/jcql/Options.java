/*
*   Copyright 2015 Dmitry Vasilyev
*
*   Licensed under the Apache License, Version 2.0 (the "License");
*   you may not use this file except in compliance with the License.
*   You may obtain a copy of the License at
*
*       http://www.apache.org/licenses/LICENSE-2.0
*
*   Unless required by applicable law or agreed to in writing, software
*   distributed under the License is distributed on an "AS IS" BASIS,
*   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
*   See the License for the specific language governing permissions and
*   limitations under the License.
*/

package com.trebogeer.jcql;


import org.kohsuke.args4j.Option;

/**
 * @author dimav
 *         Date: 1/6/15
 *         Time: 4:24 PM
 */
public class Options {

    @Option(name = "--generated-source-dir", aliases = {"-gsd"})
    String generatedSourceDir = "/tmp/generated-source-dir";

    @Option(name = "--cassandra-host", aliases = {"-h"})
    String dbHost = "localhost";

    @Option(name = "--cassandra-port", aliases = {"-P"})
    String dbPort = "9042";

    @Option(name = "--user", aliases = {"-u"})
    String userName = "";

    @Option(name = "--password", aliases = {"-p"})
    String password = "";

    @Option(name = "--keyspace", aliases = {"-k"}/*, multiValued = true*/)
    String keysapce = "jcql";

    @Option(name = "--meta-config-file", aliases = {"-mcf"})
    String config = null;

    @Option(name = "--application", aliases = {"-a"})
    String app = "catalog";

    @Option(name = "--target-package", aliases = {"-tp"})
    String jpackage = "com.trebogeer.jcql";
    
    @Option(name = "--cql-file", aliases = {"-cql"})
    String cqlFile = "/home/dimav/j-cql/jcql/src/test/resources/cql.yaml";

    @Option(name = "--debug", aliases = "-d")
    String debug = /*"n";*/"y";

}
