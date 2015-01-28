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

package com.trebogeer.jcql.dtp;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.TupleType;
import com.datastax.driver.core.UserType;
import com.sun.codemodel.JBlock;
import com.sun.codemodel.JClass;
import com.sun.codemodel.JCodeModel;
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
    protected String fname;
    protected String fnamec;
    protected String fnamecl;
    protected final String jpackage;
    protected final T data;
    protected JBlock body;

    public DataTypeProcessor(JCodeModel jcm, String jpackage, T data, JBlock body) {
        this.jcm = jcm;
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
    public JExpression processDataType(DataType dt, String fieldName) {
        this.fname = fieldName;
        this.fnamec = camelize(fieldName);
        this.fnamecl = camelize(fieldName, true);
        if (dt == null) return null;
        JExpression je = null;
        process0();
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
                mapStep0(arg0, arg1, argc0, argc1);
                if (!arg0.isCollection() && !arg1.isCollection()) {
                    mapStep1(arg0, arg1, argc0, argc1);
                } else {
                    mapStep2();
                    // not supported by cassandra yet. Ignoring for now.
                    throw new IllegalStateException("Collections of collections are not yet supported.");
                }
                mapStep3();
            } else {
                DataType arg = dt.getTypeArguments().get(0);
                if (arg == null) {
                    throw new IllegalStateException("Empty or null type arguments for list/set.");
                }
                JClass argc = getType(arg, jcm, jpackage);
                if (arg.isCollection()) {
                    // cassandra does not support embedded collections yet but might support in future
                    throw new UnsupportedOperationException("Collections of collections are not" +
                            " supported within UDTs and probably by cassandra 2.1.");
                } else if (arg.isFrozen()) {
                    if (arg instanceof UserType) {
                        UserType ut = (UserType) arg;
                        collectionStep1(ut, argc);
                    } else if (arg instanceof TupleType) {
                        TupleType tt = (TupleType) arg;
                        collectionStep2();
                    }
                } else {
                    collectionStep3();
                }

            }
        } else {

        }
        throw new IllegalStateException(String.format("Unknown type found '%s'", dt.getName().name()));
    }

    protected void mapStep3() {
    }

    protected void process0() {
    }

    protected abstract void collectionStep3();

    protected abstract void collectionStep2();

    protected abstract JExpression collectionStep1(UserType arg, JClass cl);

    protected abstract void mapStep2();

    protected abstract JExpression mapStep1(DataType arg0, DataType arg1, JClass argc0, JClass argc1);

    protected abstract void mapStep0(DataType arg0, DataType arg1, JClass argc0, JClass argc1);

    protected abstract void collectionStep0();

    protected abstract void frozenStep3();

    protected abstract void frozenStep2(TupleType tt);

    protected abstract void frozenStep1(UserType ut);

    protected abstract void frozenStep0();
}

