package com.trebogeer.jcql.dtp;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.TupleType;
import com.datastax.driver.core.UserType;
import com.sun.codemodel.JBlock;
import com.sun.codemodel.JClass;
import com.sun.codemodel.JCodeModel;
import com.sun.codemodel.JExpr;
import com.sun.codemodel.JExpression;
import com.sun.codemodel.JForEach;
import com.sun.codemodel.JVar;

import java.util.HashMap;
import java.util.Map;

import static com.trebogeer.jcql.JCQLUtils.camelize;
import static com.trebogeer.jcql.JCQLUtils.getFullClassName;

/**
 * @author dimav
 *         Date: 1/22/15
 *         Time: 5:18 PM
 */
public class MapperFieldProcessor extends DataTypeProcessor<JVar> {

    private JVar session;

    public MapperFieldProcessor(JCodeModel jcm, String fieldName, String jpackage, JVar data, JVar session, JBlock body) {
        super(jcm, fieldName, jpackage, data, body);
        this.session = session;
    }

    @Override
    protected void collectionStep3() {

    }

    @Override
    protected JExpression collectionStep2(DataType arg0, DataType arg1, JClass argc0, JClass argc1) {
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
    protected void collectionStep1(DataType arg0, DataType arg1, JClass argc0, JClass argc1) {

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
