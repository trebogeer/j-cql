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
