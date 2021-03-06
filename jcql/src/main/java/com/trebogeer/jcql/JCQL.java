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

import java.util.EnumSet;
import java.util.Set;

/**
 *
 * JCQL is a tool allowing to generate boilerplate java code from existing cassandra schema. It is intended to be
 * used with Cassandra 2.1+ due to support of UDTs (User Defined Types)/Tuples/Collections.
 * Cassandra's UDTs/Tuples/Collections and an ability to introspect schema through java driver make it possible to
 * automatically generate POJOs and corresponding mappers between database and java models. Properly generated java
 * code saves development efforts and is less error-prone compared to hand coding. Accompanied with proper CI and
 * deployment it can also guarantee consistency between database and java models at any point of application
 * lifecycle from development to production rollout. JCQL does not rely on java reflection or annotations which
 * means all discrepancies between actual cassandra schema and what client code expects it to be will be identified
 * during compilation not at runtime in the middle of the night right after production release. No need to worry
 * about Cassandra client code performance implications due to use of reflection.
 *
 *
 * @author <a href="http://github.com/trebogeer">Dmitry Vasilyev</a>
 *         Date: 1/13/15
 *         Time: 12:12 PM
 */
public class JCQL {

    public Set<JCQLKW> essential = EnumSet.allOf(JCQLKW.class);

    enum JCQLKW {

        RETURN("return", "one", "list", "set", "callback", "none"),
        CONSISTENCY("cons");

        private final String kw;
        private final String[] allowedValues;

        JCQLKW(String kw, String... allowedValues) {
            this.kw = kw;
            this.allowedValues = allowedValues;
        }

        public String kw() {
            return this.kw;
        }

        public String[] getAllowedValues() {
            return this.allowedValues;
        }

    }
}
