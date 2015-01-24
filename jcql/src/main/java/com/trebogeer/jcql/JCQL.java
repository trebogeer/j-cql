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
 * @author dimav
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
