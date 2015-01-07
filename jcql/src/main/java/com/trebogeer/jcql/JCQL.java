package com.trebogeer.jcql;


import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ColumnMetadata;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.TableMetadata;
import com.datastax.driver.core.TupleType;
import com.datastax.driver.core.UserType;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.sun.codemodel.JAnnotationValue;
import com.sun.codemodel.JClass;
import com.sun.codemodel.JClassAlreadyExistsException;
import com.sun.codemodel.JCodeModel;
import com.sun.codemodel.JDefinedClass;
import com.sun.codemodel.JExpr;
import com.sun.codemodel.JFieldRef;
import com.sun.codemodel.JFieldVar;
import com.sun.codemodel.JMethod;
import com.sun.codemodel.JMod;
import com.sun.codemodel.JType;
import com.sun.codemodel.JVar;
import com.sun.codemodel.writer.SingleStreamCodeWriter;
import org.javatuples.Pair;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Hello world!
 */

public class JCQL {

    private static final Logger logger = LoggerFactory.getLogger("JCQL.LOG");

    private Options cfg;

    private JCQL(Options cfg) {
        this.cfg = cfg;
    }

    public static void main(String[] args) {
        Options cfg = new Options();
        CmdLineParser parser = new CmdLineParser(cfg);
        try {
            parser.parseArgument(args);
        } catch (CmdLineException e) {
            throw new RuntimeException(e);
        }
        JCQL jcql = new JCQL(cfg);
        try {
            jcql.exec();
            logger.info("Done!");
        } catch (IOException e) {
            logger.error("Failed to write generated code due to : ", e);
        }

    }

    public void exec() throws IOException {
        Cluster c = Cluster.builder().addContactPoint(cfg.dbHost).build();
        Session s = c.connect(cfg.keysapces.get(0));
        Multimap<String, Pair<String, DataType>> beans = HashMultimap.create();
        Multimap<String, Pair<String, ColumnMetadata>> tables = HashMultimap.create();

        Collection<UserType> types = s.getCluster().getMetadata().getKeyspace(cfg.keysapces.get(0)).getUserTypes();

        for (UserType t : types) {
            String name = JCQLUtils.camelize(t.getTypeName());
            Set<Pair<String, DataType>> fields = new HashSet<Pair<String, DataType>>();
            for (String field : t.getFieldNames()) {
                DataType dt = t.getFieldType(field);
                fields.add(Pair.with(JCQLUtils.camelize(field, true), dt));
            }
            beans.putAll(name, fields);
        }
        Collection<TableMetadata> tbls = s.getCluster().getMetadata().getKeyspace(cfg.keysapces.get(0)).getTables();
        for (TableMetadata t : tbls) {
            String name = JCQLUtils.camelize(t.getName());
            Set<Pair<String, ColumnMetadata>> fields = new HashSet<Pair<String, ColumnMetadata>>();
            for (ColumnMetadata field : t.getColumns()) {
                fields.add(Pair.with(JCQLUtils.camelize(field.getName(), true), field));
            }
            tables.putAll(name, fields);
        }

        s.close();
        c.close();

        JCodeModel model = generateCode(beans, tables);
        if ("y".equalsIgnoreCase(cfg.debug)) {
            model.build(new SingleStreamCodeWriter(System.out));
        } else {
            File source = new File(cfg.generatedSourceDir);
            if (!source.exists()) {
                source.mkdirs();
            }
            model.build(new File(cfg.generatedSourceDir));
        }
    }

    public JCodeModel generateCode(Multimap<String, Pair<String, DataType>> beans, Multimap<String, Pair<String, ColumnMetadata>> tables) {
        JCodeModel model = new JCodeModel();
        for (String cl : beans.keySet()) {
            try {
                JDefinedClass clazz = JCQLUtils.getBeanClass(cfg.jpackage, cl, model);
                for (Pair<String, DataType> field : beans.get(cl)) {
                    javaBeanFieldWithGetterSetter(clazz,  model, field.getValue1(), field.getValue0());

                }
            } catch (JClassAlreadyExistsException e) {
                logger.warn("Class '{}' already exists for UDT, skipping ", cl);
            }

        }

        for (String table : tables.keySet()) {
            try {
                JDefinedClass clazz = JCQLUtils.getBeanClass(cfg.jpackage, table, model);
                for (Pair<String, ColumnMetadata> field : tables.get(table)) {
                    javaBeanFieldWithGetterSetter(clazz,  model, field.getValue1().getType(), field.getValue0());
                }
            } catch (JClassAlreadyExistsException ex) {
                logger.warn("Class '{}' already exists for table, skipping ", table);
            }
        }
        return model;
    }

    private void javaBeanFieldWithGetterSetter(JDefinedClass clazz, JCodeModel model, DataType dt, String name) {
        JClass ref = getType(dt, model);

        JFieldVar f = clazz.field(JMod.PRIVATE, ref, name);
        clazz.method(JMod.PUBLIC, ref, "get" + JCQLUtils.camelize(name)).body()._return(JExpr._this().ref(f));
        JMethod m = clazz.method(JMod.PUBLIC, ref, "set" + JCQLUtils.camelize(name));
        JVar p = m.param(ref, name);
        m.body().assign(JExpr._this().ref(f), p);
    }

    private JClass getType(DataType t, JCodeModel model) {
        if (t.isCollection()) {
            JClass ref = model.ref(t.asJavaClass());
            List<DataType> typeArgs = t.getTypeArguments();
            if (typeArgs.size() == 1) {
                DataType arg = typeArgs.get(0);
                if (arg instanceof UserType) {
                    UserType ut = (UserType) arg;
                    return ref.narrow(model.ref(cfg.jpackage + "." + JCQLUtils.camelize(ut.getTypeName())));
                } else if (arg instanceof TupleType) {
                    TupleType tt = (TupleType) arg;
                    // TODO implement
                }

            } else if (typeArgs.size() == 2) {
                DataType arg0 = typeArgs.get(0);
                DataType arg1 = typeArgs.get(1);
                // TODO need to look into
                ref.narrow(arg0.asJavaClass(), arg1.asJavaClass());
            }
            return ref;
        } else if (t.isFrozen()) {
            if (t instanceof UserType) {
                UserType ut = (UserType) t;
                return model.ref(cfg.jpackage + "." + JCQLUtils.camelize(ut.getTypeName()));
            } else if (t instanceof TupleType) {
                TupleType tt = (TupleType) t;
                // TODO implement
            }
            return model.ref(cfg.jpackage + "." + JCQLUtils.camelize(t.getName().name()));
        } else {
            return model.ref(t.asJavaClass());
        }
    }

}
