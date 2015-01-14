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

package com.trebogeer.jcql.it;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.TupleValue;
import org.junit.Assert;
import org.junit.Test;

import java.util.Iterator;

import static org.junit.Assert.assertEquals;

/**
 * @author dimav
 *         Date: 1/8/15
 *         Time: 3:04 PM
 */
public class SmokeIT {

    static int port = Integer.getInteger("ntPort", 9042);
    static String key_space = System.getProperty("testks", "jcql");

    @Test
    public void test0() throws Exception {
        try (
                Cluster c = Cluster.builder().addContactPoint("localhost").withPort(port).build();
                Session s = c.connect(key_space)
        ) {
            // KeyspaceMetadata r = s.getCluster().getMetadata().getKeyspace(key_space);

            s.execute("INSERT INTO jcql.tuple_test (the_key, the1_tuple) VALUES (1,('abcd'))");

            Row count = s.execute("SELECT COUNT(*) AS cnt FROM jcql.tuple_test").one();

            Long cnt = count.getLong(0);

            assertEquals(cnt, new Long(1));

            ResultSet rs = s.execute("SELECT * FROM jcql.tuple_test");

            Iterator<Row> iterator = rs.iterator();
            while (!rs.isFullyFetched() & iterator.hasNext()) {
                Row row = iterator.next();
                Integer i = row.getInt("the_key");
                assertEquals(i, new Integer(1));
                TupleValue tv = row.getTupleValue("the1_tuple");
                System.out.println(tv);
            }
            //System.out.println(r.exportAsString());

            s.execute("TRUNCATE jcql.tuple_test");

            s.close();
            c.close();
        } catch (Exception t) {
            t.printStackTrace();
            throw t;
        }
    }

}
