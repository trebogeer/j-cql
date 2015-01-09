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

    @Option(name = "--debug", aliases = "-d")
    String debug = /*"n";*/"y";

}
