package com.trebogeer.jcql.dtp;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.TupleType;
import com.datastax.driver.core.UserType;
import com.sun.codemodel.JBlock;
import com.sun.codemodel.JClass;
import com.sun.codemodel.JExpression;
import com.sun.codemodel.JVar;

import static com.trebogeer.jcql.JCQLUtils.camelize;
import static java.util.Objects.requireNonNull;

/**
 * @author <a href="http://github.com/trebogeer">Dmitry Vasilyev</a>
 *         Date: 1/26/15
 *         Time: 1:17 PM
 */
public class JCQLBindCodeFunctions {

    protected String jpackage;
    protected JVar data;
    protected JBlock body;



    public JCQLBindCodeFunctions(String jpackage, JVar data, JBlock body) {
        this.jpackage = jpackage;
        this.data = data;
        this.body = body;
    }

    public JExpression processDataType(DataType dt, String fieldName) {
        requireNonNull(dt);
        requireNonNull(fieldName);
        String fname = fieldName;
        String fnamec = camelize(fieldName);
        String fnamecl = camelize(fieldName, true);
//        if (dt == null) return null;
//        JExpression je = null;
//        process0();
//        if (dt.isFrozen()) {
//            frozenStep0();
//            if (dt instanceof UserType) {
//                UserType ut = (UserType) dt;
//                frozenStep1(ut);
//            } else if (dt instanceof TupleType) {
//                TupleType tt = (TupleType) dt;
//                frozenStep2(tt);
//            } else {
//                throw new IllegalArgumentException("Unknown or unsupported Data Type. Frozen but not user typ or tuple");
//            }
//            frozenStep3();
//        } else if (dt.isCollection()) {
//            collectionStep0();
//            if (dt.getName() == DataType.Name.MAP) {
//
//                DataType arg0 = dt.getTypeArguments().get(0);
//                DataType arg1 = dt.getTypeArguments().get(1);
//                if (arg0 == null || arg1 == null) {
//                    throw new IllegalStateException("Empty or null type arguments for map.");
//                }
//                JClass argc0 = getType(arg0, jcm, jpackage);
//                JClass argc1 = getType(arg1, jcm, jpackage);
//                mapStep0(arg0, arg1, argc0, argc1);
//                if (!arg0.isCollection() && !arg1.isCollection()) {
//                    mapStep1(arg0, arg1, argc0, argc1);
//                } else {
//                    mapStep2();
//                    // not supported by cassandra yet. Ignoring for now.
//                    throw new IllegalStateException("Collections of collections are not yet supported.");
//                }
//                mapStep3();
//            } else {
//                DataType arg = dt.getTypeArguments().get(0);
//                if (arg == null) {
//                    throw new IllegalStateException("Empty or null type arguments for list/set.");
//                }
//                JClass argc = getType(arg, jcm, jpackage);
//                if (arg.isCollection()) {
//                    // cassandra does not support embedded collections yet but might support in future
//                    throw new UnsupportedOperationException("Collections of collections are not" +
//                            " supported within UDTs and probably by cassandra 2.1.");
//                } else if (arg.isFrozen()) {
//                    if (arg instanceof UserType) {
//                        UserType ut = (UserType) arg;
//                        collectionStep1(ut, argc);
//                    } else if (arg instanceof TupleType) {
//                        TupleType tt = (TupleType) arg;
//                        collectionStep2();
//                    }
//                } else {
//                    collectionStep3();
//                }
//
//            }
//        } else {
//
//        }
        throw new IllegalStateException(String.format("Unknown type found '%s'", dt.getName().name()));
    }


}
