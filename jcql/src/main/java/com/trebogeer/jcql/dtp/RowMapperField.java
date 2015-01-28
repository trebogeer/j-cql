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
import com.datastax.driver.core.TupleValue;
import com.datastax.driver.core.UDTValue;
import com.datastax.driver.core.UserType;
import com.sun.codemodel.JBlock;
import com.sun.codemodel.JClass;
import com.sun.codemodel.JCodeModel;
import com.sun.codemodel.JExpression;
import com.sun.codemodel.JOp;
import com.sun.codemodel.JVar;

import static com.sun.codemodel.JExpr.lit;
import static com.trebogeer.jcql.JCQLUtils.getDataMethod;

/**
 * @author dimav
 *         Date: 1/23/15
 *         Time: 3:06 PM
 */
public class RowMapperField extends DataTypeProcessor<JVar> {

    public RowMapperField(JCodeModel jcm, String jpackage, JVar data, JBlock body) {
        super(jcm, jpackage, data, body);
    }

    @Override
    protected void process0() {
        JBlock ifNotNullBody = body._if(JOp.not(data.invoke("isNull")
                .arg(lit(fname))))._then();
        this.body = ifNotNullBody;

    }

    @Override
    protected void collectionStep3() {

    }

    @Override
    protected void collectionStep2() {

    }

    @Override
    protected JExpression collectionStep1(UserType arg, JClass cl) {
        JExpression tclass = arg.isFrozen() ? arg.asJavaClass().isAssignableFrom(UDTValue.class) ?
                jcm.ref(UDTValue.class).dotclass() : jcm.ref(TupleValue.class).dotclass()
                : cl.dotclass();
        return data.invoke(getDataMethod(arg.getName()))
                .arg(fname)
                .arg(tclass);
    }

    @Override
    protected void mapStep2() {

    }

    @Override
    protected JExpression mapStep1(DataType arg0, DataType arg1, JClass argc0, JClass argc1) {
        return null;
    }

    @Override
    protected void mapStep0(DataType arg0, DataType arg1, JClass argc0, JClass argc1) {

    }

    @Override
    protected void collectionStep0() {

    }

    @Override
    protected void frozenStep3() {

    }

    @Override
    protected void frozenStep2(TupleType tt) {

    }

    @Override
    protected void frozenStep1(UserType ut) {

    }

    @Override
    protected void frozenStep0() {

    }
}
