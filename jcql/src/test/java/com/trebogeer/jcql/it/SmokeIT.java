package com.trebogeer.jcql.it;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import org.junit.Test;

import java.util.Iterator;

/**
 * @author dimav
 *         Date: 1/8/15
 *         Time: 3:04 PM
 */
public class SmokeIT {

    static int port = Integer.getInteger("ntPort", 9042);
    static String key_space = System.getProperty("testks", "jcql");

    @Test
    public void test() throws Exception {
        try {
            Cluster c = Cluster.builder().addContactPoint("localhost").withPort(port).build();
            Session s = c.connect();
            KeyspaceMetadata r = s.getCluster().getMetadata().getKeyspace(key_space);

            s.execute("INSERT INTO jcql.phone VALUES ('work','623-34-5687')");

            ResultSet rs = s.execute("SELECT * FORM jcql.phone");

            Iterator<Row> iterator = rs.iterator();
            while (!rs.isFullyFetched() & iterator.hasNext()) {
                Row row = iterator.next();
                System.out.println(row.getString("alias"));
                System.out.println(row.getString("number"));
            }
            System.out.println(r.exportAsString());

            s.close();
            c.close();
        } catch (Exception t) {
            t.printStackTrace();
            throw t;
        }
    }

}
