package com.trebogeer.jcql.dtp;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.TupleType;
import com.datastax.driver.core.UserType;
import com.sun.codemodel.JBlock;
import com.sun.codemodel.JClass;
import com.sun.codemodel.JCodeModel;
import com.sun.codemodel.JExpr;
import com.sun.codemodel.JExpression;
import com.sun.codemodel.JVar;

import static com.trebogeer.jcql.JCQLUtils.camelize;
import static com.trebogeer.jcql.JCQLUtils.getType;

/**
 * @author dimav
 *         Date: 1/22/15
 *         Time: 5:10 PM
 */
public abstract class DataTypeProcessor<T extends JVar> {

    protected final JCodeModel jcm;
    protected final String fname;
    protected final String fnamec;
    protected final String fnamecl;
    protected final String jpackage;
    protected final T data;
    protected final JBlock body;

    public DataTypeProcessor(JCodeModel jcm, String fieldName, String jpackage, T data, JBlock body) {
        this.jcm = jcm;
        this.fname = fieldName;
        this.fnamec = camelize(fieldName);
        this.fnamecl = camelize(fieldName, true);
        this.jpackage = jpackage;
        this.data = data;
        this.body = body;
    }

    /**
     * skeleton - need to try to extract common processing flow for all data types and utilize recursion for embedded
     * data types
     *
     * @param dt cassandra data type ot process
     */
    public JExpression processDataType(DataType dt) {
        if (dt == null) return null;
        JExpression je = null;
        if (dt.isFrozen()) {
            frozenStep0();
            if (dt instanceof UserType) {
                UserType ut = (UserType) dt;
                frozenStep1(ut);
            } else if (dt instanceof TupleType) {
                TupleType tt = (TupleType) dt;
                frozenStep2(tt);
            } else {
                throw new IllegalArgumentException("Unknown or unsupported Data Type. Frozen but not user typ or tuple");
            }
            frozenStep3();
        } else if (dt.isCollection()) {
            collectionStep0();
            if (dt.getName() == DataType.Name.MAP) {

                DataType arg0 = dt.getTypeArguments().get(0);
                DataType arg1 = dt.getTypeArguments().get(1);
                if (arg0 == null || arg1 == null) {
                    throw new IllegalStateException("Empty or null type arguments for map.");
                }
                JClass argc0 = getType(arg0, jcm, jpackage);
                JClass argc1 = getType(arg1, jcm, jpackage);
                collectionStep1(arg0, arg1, argc0, argc1);
                if (arg0.isFrozen() || arg1.isFrozen()) {
                collectionStep2(arg0, arg1, argc0, argc1);
                } else if (arg0.isCollection() || arg1.isCollection()) {
                    // TODO not supported by cassandra yet. Ignoring for now.
                    throw new IllegalStateException("Collections of collections are not yet supported.");
                } else {
                    // TODO process simple types
                }
                collectionStep3();
            } else {
                DataType arg = dt.getTypeArguments().get(0);
                if (arg == null) {
                    throw new IllegalStateException("Empty or null type arguments for list/set.");
                }
                JClass argc = getType(arg, jcm, jpackage);
                if (arg.isCollection()) {
                    // TODO cassandra does not support embedded collections yet but might support in future
                    throw new UnsupportedOperationException("Collections of collections are not" +
                            " supported within UDTs and probably by cassandra 2.1.");
                } else if (arg.isFrozen()) {

                }

            }
        } else {

        }
        throw new IllegalStateException(String.format("Unknown type found '%s'", dt.getName().name()));
    }

    protected abstract void collectionStep3();

    protected abstract JExpression collectionStep2(DataType arg0, DataType arg1, JClass argc0, JClass argc1);

    protected abstract void collectionStep1(DataType arg0, DataType arg1, JClass argc0, JClass argc1);

    protected abstract void collectionStep0();

    protected abstract void frozenStep3();

    protected abstract void frozenStep2(TupleType tt);

    protected abstract void frozenStep1(UserType ut);

    protected abstract void frozenStep0();
}

