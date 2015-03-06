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
import com.datastax.driver.core.UDTValue;
import com.datastax.driver.core.UserType;
import com.sun.codemodel.JBlock;
import com.sun.codemodel.JClass;
import com.sun.codemodel.JCodeModel;
import com.sun.codemodel.JExpr;
import com.sun.codemodel.JExpression;
import com.sun.codemodel.JForEach;
import com.sun.codemodel.JInvocation;
import com.sun.codemodel.JVar;

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;

import static com.trebogeer.jcql.JCQLUtils.camelize;
import static com.trebogeer.jcql.JCQLUtils.getFullClassName;

/**
 * @author <a href="http://github.com/trebogeer">Dmitry Vasilyev</a>
 *         Date: 1/22/15
 *         Time: 5:18 PM
 */
public class MapperFieldProcessor extends DataTypeProcessor<JVar> {

    private JVar session;

    public MapperFieldProcessor(JCodeModel jcm, String jpackage, JVar data, JVar session, JBlock body) {
        super(jcm, jpackage, data, body);
        this.session = session;
    }

    @Override
    protected void collectionStep3() {

    }

    @Override
    protected void collectionStep2() {

    }

    @Override
    protected JExpression collectionStep1(UserType arg, JClass collectionClass) {
        String utname = arg.getTypeName();
        String utnamec = camelize(utname);
        JClass sourceCollectionGeneric = jcm.ref(getFullClassName(jpackage, utnamec));
        JClass sourceCollectionClass = jcm.ref(arg.asJavaClass())
                .narrow(sourceCollectionGeneric);

        JVar source = body.decl(sourceCollectionClass, fnamecl + "Source",
                data.invoke("get" + camelize(fname)));
        JVar target = body.decl(jcm.ref(arg.asJavaClass()).narrow(UDTValue.class),
                fnamecl + "Target",
                JExpr._new(arg.asJavaClass().isAssignableFrom(Set.class)
                        ? jcm.ref(HashSet.class).narrow(UDTValue.class)
                        : jcm.ref(LinkedList.class).narrow(UDTValue.class)));
        JForEach forEach = body.forEach(sourceCollectionGeneric, "entry", source);
        JVar entry = forEach.var();
        JBlock forEachBody = forEach.body();

        JInvocation convertToUDT = sourceCollectionGeneric
                .staticInvoke("udtMapper").invoke("toUDT").arg(entry).arg(session);
        forEachBody.add(target.invoke("add").arg(convertToUDT));
        return target;
    }

    @Override
    protected void mapStep2() {

    }

    @Override
    protected JExpression mapStep1(DataType arg0, DataType arg1, JClass argc0, JClass argc1) {
        JVar source = body.decl(jcm.ref(Map.class).narrow(argc0, argc1),
                fnamecl + "Source", data.invoke("get" + camelize(fname)));
        JVar target = body.decl(jcm.ref(Map.class).narrow(arg0.asJavaClass(), arg1.asJavaClass()),
                fnamecl + "Target",
                JExpr._new(jcm.ref(HashMap.class).narrow(arg0.asJavaClass(), arg1.asJavaClass())));
        JClass entryClass = jcm.ref(Map.Entry.class).narrow(argc0, argc1);
        JForEach forEach = body.forEach(entryClass, "entry", source.invoke("entrySet"));
        JVar entry = forEach.var();
        JExpression k = entry.invoke("getKey");
        JExpression v = entry.invoke("getValue");
        JBlock forEachBody = forEach.body();
        JExpression key = arg0.isFrozen()
                ? argc0.staticInvoke("udtMapper").invoke("toUDT").arg(k).arg(session)
                : k;
        JExpression value = arg1.isFrozen()
                ? argc1.staticInvoke("udtMapper").invoke("toUDT").arg(v).arg(session)
                : v;
        forEachBody.add(target.invoke("put").arg(key).arg(value));
        return target;
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
        jcm.ref(getFullClassName(jpackage, (ut).getTypeName()))
                .staticInvoke("udtMapper")
                .invoke("toUDT").arg(data.invoke("get" + fnamec)).arg(session);
    }

    @Override
    protected void frozenStep0() {

    }
}
