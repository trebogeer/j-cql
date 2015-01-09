package com.trebogeer.jcql;


import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ColumnMetadata;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.TableMetadata;
import com.datastax.driver.core.TupleType;
import com.datastax.driver.core.UserType;
import com.datastax.driver.mapping.annotations.Column;
import com.datastax.driver.mapping.annotations.Table;
import com.datastax.driver.mapping.annotations.UDT;
import com.google.common.base.Function;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Collections2;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.sun.codemodel.ClassType;
import com.sun.codemodel.JBlock;
import com.sun.codemodel.JClass;
import com.sun.codemodel.JClassAlreadyExistsException;
import com.sun.codemodel.JCodeModel;
import com.sun.codemodel.JDefinedClass;
import com.sun.codemodel.JExpr;
import com.sun.codemodel.JFieldVar;
import com.sun.codemodel.JMethod;
import com.sun.codemodel.JMod;
import com.sun.codemodel.JTypeVar;
import com.sun.codemodel.JVar;
import com.sun.codemodel.writer.SingleStreamCodeWriter;
import org.javatuples.Pair;
import org.javatuples.Triplet;
import org.javatuples.Tuple;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.lang.annotation.Annotation;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Hello world!
 */

public class JCQL {

    private static final Logger logger = LoggerFactory.getLogger("JCQL.LOG");

    private final Options cfg;
    private final JCodeModel model;

    private JCQL(Options cfg) {
        this.cfg = cfg;
        this.model = new JCodeModel();
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
        String keyspace = cfg.keysapce;
        Cluster c = Cluster.builder().addContactPoint(cfg.dbHost).withPort(Integer.valueOf(cfg.dbPort)).build();
        Session s = c.connect(keyspace);
        Multimap<String, Pair<String, DataType>> beans = HashMultimap.create();
        Multimap<String, Pair<String, ColumnMetadata>> tables = HashMultimap.create();
        ArrayListMultimap<String, String> partitionKeys = ArrayListMultimap.create();

        Collection<UserType> types = s.getCluster().getMetadata().getKeyspace(keyspace).getUserTypes();

        for (UserType t : types) {
            String name = t.getTypeName();
            Set<Pair<String, DataType>> fields = new HashSet<Pair<String, DataType>>();
            for (String field : t.getFieldNames()) {
                DataType dt = t.getFieldType(field);
                fields.add(Pair.with(field, dt));
            }
            beans.putAll(name, fields);
        }
        Collection<TableMetadata> tbls = s.getCluster().getMetadata().getKeyspace(keyspace).getTables();
        for (TableMetadata t : tbls) {
            String name = t.getName();
            for (ColumnMetadata clmdt : t.getPartitionKey()) {
                partitionKeys.put(name, clmdt.getName());
            }
            partitionKeys.trimToSize();
            Set<Pair<String, ColumnMetadata>> fields = new HashSet<Pair<String, ColumnMetadata>>();
            for (ColumnMetadata field : t.getColumns()) {
                fields.add(Pair.with(field.getName(), field));
            }
            tables.putAll(name, fields);
        }

        s.close();
        c.close();

        JCodeModel model = generateCode(beans, tables, partitionKeys);
        if ("y".equalsIgnoreCase(cfg.debug)) {
            model.build(new SingleStreamCodeWriter(System.out));
        } else {
            File source = new File(cfg.generatedSourceDir);
            if (source.exists() || source.mkdirs()) {
                model.build(new File(cfg.generatedSourceDir));
            }
        }
    }

    public JCodeModel generateCode(
            Multimap<String, Pair<String, DataType>> beans,
            Multimap<String, Pair<String, ColumnMetadata>> tables,
            ArrayListMultimap<String, String> partitionKeys) {
        // final JDefinedClass rowMapperFactory;
        final JDefinedClass rowMapper;
        try {
            rowMapper = model._class(JMod.PUBLIC, cfg.jpackage + ".RowMapper", ClassType.INTERFACE);
            JTypeVar jtv = rowMapper.generify("T");
            rowMapper.method(JMod.NONE, jtv, "map").param(com.datastax.driver.core.GettableData.class, "data");
        } catch (Exception e) {
            throw new RuntimeException("Failed to generate mapper interface.", e);
        }

     /*   try {
            rowMapperFactory = model._class(JMod.PUBLIC, cfg.jpackage + ".RowMapperFactory", ClassType.INTERFACE);
            JTypeVar jtv = rowMapperFactory.generify("RowMapper<T>");
            rowMapperFactory.method(JMod.NONE, jtv, " mapper");
        } catch (Exception e) {
            throw new RuntimeException("Failed to generate mapper factory interface.", e);
        }*/


        for (String cl : beans.keySet()) {
            try {
                JDefinedClass clazz = JCQLUtils.getBeanClass(cfg.jpackage, JCQLUtils.camelize(cl), model);

                // row mapper
                rowMapperCode(clazz, rowMapper, beans.get(cl));

                // fields/getters/setters/annotations
                clazz.annotate(UDT.class).param("keyspace", cfg.keysapce).param("name", cl);
                for (Pair<String, DataType> field : beans.get(cl)) {
                    javaBeanFieldWithGetterSetter(clazz, field.getValue1(), field.getValue0(),
                            -1, com.datastax.driver.mapping.annotations.Field.class);

                }
            } catch (JClassAlreadyExistsException e) {
                logger.warn("Class '{}' already exists for UDT, skipping ", cl);
            }

        }

        for (String table : tables.keySet()) {
            try {
                JDefinedClass clazz = JCQLUtils.getBeanClass(cfg.jpackage, JCQLUtils.camelize(table), model);
                // row mapper
                rowMapperCode(clazz, rowMapper, Collections2.transform(tables.get(table), new Function<Pair<String, ColumnMetadata>, Pair<String, DataType>>() {
                    @Override
                    public Pair<String, DataType> apply(Pair<String, ColumnMetadata> input) {
                        return Pair.with(input.getValue0(), input.getValue1().getType());
                    }
                }));

                // fields/getters/setters/annotations
                clazz.annotate(Table.class).param("keyspace", cfg.keysapce).param("name", table);
                List<String> pkList = partitionKeys.get(table);
                Set<String> pks = new HashSet<String>(pkList);

                for (Pair<String, ColumnMetadata> field : tables.get(table)) {
                    String fieldName = field.getValue0();
                    int order = 0;
                    if (pks.contains(fieldName) && pks.size() > 1) {
                        order = pkList.indexOf(field.getValue0());
                    }
                    javaBeanFieldWithGetterSetter(clazz, field.getValue1().getType(), fieldName,
                            order, Column.class);


                }
            } catch (JClassAlreadyExistsException ex) {
                logger.warn("Class '{}' already exists for table, skipping ", table);
            }
        }
        return model;
    }

    private void javaBeanFieldWithGetterSetter(
            JDefinedClass clazz,
            DataType dt, String name, int pko,
            Class<? extends Annotation> ann) {
        JClass ref = getType(dt);
        JFieldVar f = clazz.field(JMod.PRIVATE, ref, JCQLUtils.camelize(name, true));
        if (ann != null) {
            f.annotate(ann).param("name", name);
        }
        if (dt.isFrozen()) {
            f.annotate(com.datastax.driver.mapping.annotations.Frozen.class);
        }
        if (pko == 0) {
            f.annotate(com.datastax.driver.mapping.annotations.PartitionKey.class);
        } else if (pko > 0) {
            f.annotate(com.datastax.driver.mapping.annotations.PartitionKey.class).param("value", pko);
        }
        clazz.method(JMod.PUBLIC, ref, "get" + JCQLUtils.camelize(name)).body()._return(JExpr._this().ref(f));
        JMethod m = clazz.method(JMod.PUBLIC, ref, "set" + JCQLUtils.camelize(name));
        JVar p = m.param(ref, JCQLUtils.camelize(name, true));
        m.body().assign(JExpr._this().ref(f), p);
    }

    private void rowMapperCode(JDefinedClass clazz, JClass rowMapper, Collection<Pair<String, DataType>> fields) throws JClassAlreadyExistsException {
        JClass rowMapperNarrowed = rowMapper.narrow(clazz);
        //  clazz._implements(rowMapperFactory.narrow(rowMapperNarrowed));
        JDefinedClass mapperImpl = clazz._class(
                JMod.FINAL | JMod.STATIC | JMod.PRIVATE, clazz.name() + "RowMapper")
                ._implements(rowMapperNarrowed);
        clazz.field(JMod.PRIVATE | JMod.STATIC | JMod.FINAL, mapperImpl, "mapper", JExpr._new(mapperImpl));

        JMethod map = mapperImpl.method(JMod.PUBLIC, clazz, "map");
        JVar param = map.param(com.datastax.driver.core.GettableData.class, "data");
        JBlock body = map.body();
        body._if(param.eq(JExpr._null()))._then()._return(JExpr._null());
        JVar bean = body.decl(clazz, "entity", JExpr._new(clazz));
        for (Pair<String, DataType> field : fields) {
            String name = field.getValue0();
            DataType type = field.getValue1();
            if (type.isCollection()) {
                // TODO handle class parameters

            } else if (type.isFrozen()) {
                if (type instanceof UserType) {
                    UserType ut = (UserType) type;
                    body.add(bean.invoke("set" + JCQLUtils.camelize(name))
                            .arg(model.ref(getFullCallName(ut.getTypeName())).staticInvoke("mapper").invoke("map")
                                    .arg(param.invoke(JCQLUtils.getDataMethod(type.getName())).arg(name))));
                } else if (type instanceof TupleType) {

                }
                // TODO tuple and udt
            } else {
                body.add(bean.invoke("set" + JCQLUtils.camelize(name))
                        .arg(param.invoke(JCQLUtils.getDataMethod(type.getName())).arg(name)));
            }
        }
        body._return(JExpr.direct("entity"));
        clazz.method(JMod.PUBLIC | JMod.STATIC, rowMapperNarrowed, "mapper").body()._return(JExpr.direct("mapper"));
    }

    private JClass getType(DataType t) {
        if (t.isCollection()) {
            JClass ref = model.ref(t.asJavaClass());
            List<DataType> typeArgs = t.getTypeArguments();
            if (typeArgs.size() == 1) {
                DataType arg = typeArgs.get(0);
                return ref.narrow(getType(arg));
            } else if (typeArgs.size() == 2) {
                DataType arg0 = typeArgs.get(0);
                DataType arg1 = typeArgs.get(1);
                JClass argc0 = getType(arg0);
                JClass argc1 = getType(arg1);
                return ref.narrow(argc0, argc1);
            }
            return ref;
        } else if (t.isFrozen()) {
            if (t instanceof UserType) {
                UserType ut = (UserType) t;
                return model.ref(getFullCallName(ut.getTypeName()));
            } else if (t instanceof TupleType) {
                // TODO figure out how cassandra standard mappers deal with tuples
                // and what are they mapped to  -- seems like they don't handle tuples
                TupleType tt = (TupleType) t;
                List<DataType> dt = tt.getComponentTypes();
                JClass dts[] = new JClass[dt.size()];
                for (int i = 0; i < dts.length; i++) {
                    dts[i] = getType(dt.get(i));
                }
                return model.ref(JCQLUtils.getTupleClass(dts.length)).narrow(dts);
            }

        }
        return model.ref(t.asJavaClass());
    }

    private String getFullCallName(String name) {
        return cfg.jpackage + "." + JCQLUtils.camelize(name);
    }

}
