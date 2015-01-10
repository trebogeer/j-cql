package com.trebogeer.jcql;

import com.datastax.driver.core.DataType;
import com.sun.codemodel.ClassType;
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
import org.javatuples.Tuple;
import org.javatuples.Unit;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author dimav
 *         Date: 1/6/15
 *         Time: 4:55 PM
 */
public class JCQLUtils {

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
}
