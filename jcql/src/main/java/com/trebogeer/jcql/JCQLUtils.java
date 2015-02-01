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

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.TupleType;
import com.datastax.driver.core.UserType;
import com.sun.codemodel.ClassType;
import com.sun.codemodel.JClass;
import com.sun.codemodel.JClassAlreadyExistsException;
import com.sun.codemodel.JCodeModel;
import com.sun.codemodel.JDefinedClass;
import com.sun.codemodel.JMod;
import org.javatuples.Decade;
import org.javatuples.Ennead;
import org.javatuples.Octet;
import org.javatuples.Pair;
import org.javatuples.Quartet;
import org.javatuples.Quintet;
import org.javatuples.Septet;
import org.javatuples.Sextet;
import org.javatuples.Triplet;
import org.javatuples.Unit;

import java.util.List;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author <a href="http://github.com/trebogeer">Dmitry Vasilyev</a>
 *         Date: 1/6/15
 *         Time: 4:55 PM
 */
public class JCQLUtils {

    public static final String POM = "<project xmlns=\"http://maven.apache.org/POM/4.0.0\" xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\"\n" +
            "         xsi:schemaLocation=\"http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd\">\n" +
            "    <modelVersion>4.0.0</modelVersion>\n" +
            "\n" +
            "    <groupId>${group.id}</groupId>\n" +
            "    <artifactId>${artifact.id}</artifactId>\n" +
            "    <version>${date}</version>\n" +
            "    <packaging>jar</packaging>\n" +
            "\n" +
            "    <name>${artifact.id}</name>\n" +
            "\n" +
            "    <properties>\n" +
            "        <project.build.sourceEncoding>UTF-8</project.build.sourceEncoding>\n" +
            "    </properties>\n" +
            "\n" +
            "    <dependencies>\n" +
            "        <dependency>\n" +
            "            <groupId>org.javatuples</groupId>\n" +
            "            <artifactId>javatuples</artifactId>\n" +
            "            <version>1.2</version>\n" +
            "        </dependency>\n" +
            "        <dependency>\n" +
            "            <groupId>com.datastax.cassandra</groupId>\n" +
            "            <artifactId>cassandra-driver-core</artifactId>\n" +
            "            <version>2.1.4</version>\n" +
            "        </dependency>\n" +
            "    </dependencies>\n" +
            "</project>";

    public static String camelize(String word) {
        return camelize(word, false);
    }

    public static String camelize(String word, boolean lowercaseFirstLetter) {
        // Replace all slashes with dots (package separator)
        Pattern p = Pattern.compile("\\/(.?)");
        Matcher m = p.matcher(word);
        while (m.find()) {
            word = m.replaceFirst("." + m.group(1)/*.toUpperCase()*/);
            m = p.matcher(word);
        }

        // Uppercase the class name.
        p = Pattern.compile("(\\.?)(\\w)([^\\.]*)$");
        m = p.matcher(word);
        if (m.find()) {
            String rep = m.group(1) + m.group(2).toUpperCase() + m.group(3);
            rep = rep.replaceAll("\\$", "\\\\\\$");
            word = m.replaceAll(rep);
        }

        // Replace two underscores with $ to support inner classes.
        p = Pattern.compile("(__)(.)");
        m = p.matcher(word);
        while (m.find()) {
            word = m.replaceFirst("\\$" + m.group(2).toUpperCase());
            m = p.matcher(word);
        }

        // Remove all underscores
        p = Pattern.compile("(_)(.)");
        m = p.matcher(word);
        while (m.find()) {
            word = m.replaceFirst(m.group(2).toUpperCase());
            m = p.matcher(word);
        }

        if (lowercaseFirstLetter) {
            word = word.substring(0, 1).toLowerCase() + word.substring(1);
        }

        return word;
    }


    public static JDefinedClass getBeanClass(String packageName, String bean, JCodeModel codeModel) throws JClassAlreadyExistsException {
        JDefinedClass clazz = codeModel
                ._class(JMod.PUBLIC, packageName + "." + bean, ClassType.CLASS);
        clazz.constructor(JMod.PUBLIC);
        return clazz;
    }

    public static String getDataMethod(DataType.Name dataTypeName) {

        switch (dataTypeName) {
            case ASCII:
            case TEXT:
            case VARCHAR:
                return "getString";
            case BLOB:
                return "getBytes";
            case CUSTOM:
                return "getBytesUnsafe";
            case BIGINT:
            case COUNTER:
                return "getLong";
            case INT:
                return "getInt";
            case DECIMAL:
                return "getDecimal";
            case VARINT:
                return "getVarint";
            case BOOLEAN:
                return "getBool";
            case DOUBLE:
                return "getDouble";
            case TIMESTAMP:
                return "getDate";
            case FLOAT:
                return "getFloat";
            case UUID:
            case TIMEUUID:
                return "getUUID";
            case INET:
                return "getInet";
            case LIST:
                return "getList";
            case SET:
                return "getSet";
            case MAP:
                return "getMap";
            case UDT:
                return "getUDTValue";
            case TUPLE:
                return "getTupleValue";
        }
        return null;
    }

    public static String setDataMethod(DataType.Name dataTypeName) {

        switch (dataTypeName) {
            case ASCII:
            case TEXT:
            case VARCHAR:
                return "setString";
            case BLOB:
                return "setBytes";
            case CUSTOM:
                return "setBytesUnsafe";
            case BIGINT:
            case COUNTER:
                return "setLong";
            case INT:
                return "setInt";
            case DECIMAL:
                return "setDecimal";
            case VARINT:
                return "setVarint";
            case BOOLEAN:
                return "setBool";
            case DOUBLE:
                return "setDouble";
            case TIMESTAMP:
                return "setDate";
            case FLOAT:
                return "setFloat";
            case UUID:
            case TIMEUUID:
                return "setUUID";
            case INET:
                return "setInet";
            case LIST:
                return "setList";
            case SET:
                return "setSet";
            case MAP:
                return "setMap";
            case UDT:
                return "setUDTValue";
            case TUPLE:
                return "setTupleValue";
        }
        return null;
    }

    public static Class<?> getTupleClass(int len) {
        switch (len) {
            case 1:
                return Unit.class;
            case 2:
                return Pair.class;
            case 3:
                return Triplet.class;
            case 4:
                return Quartet.class;
            case 5:
                return Quintet.class;
            case 6:
                return Sextet.class;
            case 7:
                return Septet.class;
            case 8:
                return Octet.class;
            case 9:
                return Ennead.class;
            case 10:
                return Decade.class;
            default:
                throw new IllegalArgumentException(String.format("Unsupported value %d", len));

        }
    }


    public static boolean isInteger(String s) {
        return isInteger(s, 10);
    }

    public static boolean isInteger(String s, int radix) {
        if (s.isEmpty()) return false;
        for (int i = 0; i < s.length(); i++) {
            if (i == 0 && s.charAt(i) == '-') {
                if (s.length() == 1) return false;
                else continue;
            }
            if (Character.digit(s.charAt(i), radix) < 0) return false;
        }
        return true;
    }
    @Deprecated
    public static JClass getType(DataType t, JCodeModel model, Options cfg) {
        if (t.isCollection()) {
            JClass ref = model.ref(t.asJavaClass());
            List<DataType> typeArgs = t.getTypeArguments();
            if (typeArgs.size() == 1) {
                DataType arg = typeArgs.get(0);
                return ref.narrow(getType(arg, model, cfg));
            } else if (typeArgs.size() == 2) {
                DataType arg0 = typeArgs.get(0);
                DataType arg1 = typeArgs.get(1);
                JClass argc0 = getType(arg0, model, cfg);
                JClass argc1 = getType(arg1, model, cfg);
                return ref.narrow(argc0, argc1);
            }
            return ref;
        } else if (t.isFrozen()) {
            if (t instanceof UserType) {
                UserType ut = (UserType) t;
                return model.ref(getFullClassName(cfg.jpackage, ut.getTypeName()));
            } else if (t instanceof TupleType) {
                // -- seems like datastax client doesn't handle tuples - dealing with it
                TupleType tt = (TupleType) t;
                List<DataType> dt = tt.getComponentTypes();
                JClass dts[] = new JClass[dt.size()];
                for (int i = 0; i < dts.length; i++) {
                    dts[i] = getType(dt.get(i), model, cfg);
                }
                return model.ref(getTupleClass(dts.length)).narrow(dts);
            }

        }
        return model.ref(t.asJavaClass());
    }

    public static JClass getType(DataType t, JCodeModel model, String jpackage) {
        if (t.isCollection()) {
            JClass ref = model.ref(t.asJavaClass());
            List<DataType> typeArgs = t.getTypeArguments();
            if (typeArgs.size() == 1) {
                DataType arg = typeArgs.get(0);
                return ref.narrow(getType(arg, model, jpackage));
            } else if (typeArgs.size() == 2) {
                DataType arg0 = typeArgs.get(0);
                DataType arg1 = typeArgs.get(1);
                JClass argc0 = getType(arg0, model, jpackage);
                JClass argc1 = getType(arg1, model, jpackage);
                return ref.narrow(argc0, argc1);
            }
            return ref;
        } else if (t.isFrozen()) {
            if (t instanceof UserType) {
                UserType ut = (UserType) t;
                return model.ref(getFullClassName(jpackage, ut.getTypeName()));
            } else if (t instanceof TupleType) {
                // -- seems like datastax client doesn't handle tuples - dealing with it
                TupleType tt = (TupleType) t;
                List<DataType> dt = tt.getComponentTypes();
                JClass dts[] = new JClass[dt.size()];
                for (int i = 0; i < dts.length; i++) {
                    dts[i] = getType(dt.get(i), model, jpackage);
                }
                return model.ref(getTupleClass(dts.length)).narrow(dts);
            }

        }
        return model.ref(t.asJavaClass());
    }

    public static String getFullClassName(String jpackage, String name) {
        return jpackage + "." + camelize(name);
    }

    public static String typeToDTStaticMthod(DataType.Name name) {
        switch (name) {
            case ASCII:
                return "ascii";
            case BIGINT:
                return "bigint";
            case BLOB:
                return "blob";
            case BOOLEAN:
                return "cboolean";
            case COUNTER:
                return "counter";
            case DECIMAL:
                return "decimal";
            case DOUBLE:
                return "cdouble";
            case FLOAT:
                return "cfloat";
            case INET:
                return "inet";
            case INT:
                return "cint";
            case TEXT:
                return "text";
            case TIMESTAMP:
                return "timestamp";
            case UUID:
                return "uuid";
            case VARCHAR:
                return "varchar";
            case VARINT:
                return "varint";
            case TIMEUUID:
                return "timeuuid";
            case LIST:
                return "list";
            case SET:
                return "set";
            case MAP:
                return "map";
            case CUSTOM:
                return "custom";
            /*case UDT:     should be processed separately
             case TUPLE:    should be processed separately
            */
            default:
                return null;
        }

    }
}
