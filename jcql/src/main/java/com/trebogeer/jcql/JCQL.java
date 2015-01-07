package com.trebogeer.jcql;


import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.UserType;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.sun.codemodel.JClass;
import com.sun.codemodel.JClassAlreadyExistsException;
import com.sun.codemodel.JCodeModel;
import com.sun.codemodel.JDefinedClass;
import com.sun.codemodel.JMod;
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

        s.close();
        c.close();

        JCodeModel model = generateCode(beans);
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

    public JCodeModel generateCode(Multimap<String, Pair<String, DataType>> beans) {
        JCodeModel model = new JCodeModel();
        for (String cl : beans.keySet()) {
            try {
                JDefinedClass clazz = JCQLUtils.getBeanClass(cfg.jpackage, cl, model);
                for (Pair<String, DataType> field : beans.get(cl)) {
                    JClass ref = model.ref(getType(field.getValue1()));
                    if (field.getValue1().isCollection()) {
                        List<DataType> typeArgs = field.getValue1().getTypeArguments();
                        if (typeArgs.size() == 1) {
                            DataType arg = typeArgs.get(0);
                            ref = ref.narrow(arg.asJavaClass());
                        } else if (typeArgs.size() == 2) {
                            DataType arg0 = typeArgs.get(0);
                            DataType arg1 = typeArgs.get(1);
                            ref.narrow(arg0.asJavaClass(), arg1.asJavaClass());
                        }
                    }
                    clazz.field(JMod.PRIVATE, ref, field.getValue0());

                }
            } catch (JClassAlreadyExistsException e) {
                logger.warn("Class '{}' already exists, skipping ", cl);
            }

        }
        return model;
    }

    private Class<?> getType(DataType t) {
        if (t.isCollection()) {
            return t.asJavaClass();
        } else if (t.isFrozen()) {
            // TODO implement properly
            return Object.class;
        } else {
            return t.asJavaClass();
        }
    }

}
