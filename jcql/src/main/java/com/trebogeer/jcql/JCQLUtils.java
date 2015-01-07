package com.trebogeer.jcql;

import com.sun.codemodel.ClassType;
import com.sun.codemodel.JClassAlreadyExistsException;
import com.sun.codemodel.JCodeModel;
import com.sun.codemodel.JDefinedClass;
import com.sun.codemodel.JMod;
import com.sun.codemodel.JVar;

import javax.sql.DataSource;
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
}
