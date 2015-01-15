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


package com.trebogeer.jcql;


import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.ColumnMetadata;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.PlainTextAuthProvider;
import com.datastax.driver.core.PreparedId;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.TableMetadata;
import com.datastax.driver.core.TupleType;
import com.datastax.driver.core.TupleValue;
import com.datastax.driver.core.UDTValue;
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
import com.sun.codemodel.JConditional;
import com.sun.codemodel.JDefinedClass;
import com.sun.codemodel.JExpr;
import com.sun.codemodel.JExpression;
import com.sun.codemodel.JFieldVar;
import com.sun.codemodel.JForEach;
import com.sun.codemodel.JInvocation;
import com.sun.codemodel.JMethod;
import com.sun.codemodel.JMod;
import com.sun.codemodel.JOp;
import com.sun.codemodel.JPackage;
import com.sun.codemodel.JTypeVar;
import com.sun.codemodel.JVar;
import com.sun.codemodel.writer.SingleStreamCodeWriter;
import org.javatuples.Pair;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.trebogeer.jcql.JCQLUtils.camelize;
import static com.trebogeer.jcql.JCQLUtils.getDataMethod;
import static com.trebogeer.jcql.JCQLUtils.getTupleClass;
import static com.trebogeer.jcql.JCQLUtils.isInteger;

/**
 * @author Dmitry Vasilyev
 */

public class JCQLMain {

    private static final Logger logger = LoggerFactory.getLogger("JCQL.LOG");

    private final Options cfg;
    private final JCodeModel model;

    private JCQLMain(Options cfg) {
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
        final JCQLMain jcql = new JCQLMain(cfg);
        try {
            jcql.exec();
            logger.info("Done!");
        } catch (IOException e) {
            logger.error("Failed to write generated code due to : ", e);
        }

    }

    /**
     * main routine generating java code
     *
     * @throws IOException
     */
    public void exec() throws IOException {
        String keyspace = cfg.keysapce;
        Cluster.Builder b = Cluster.builder()
                .addContactPoint(cfg.dbHost)
                .withPort(Integer.valueOf(cfg.dbPort));
        if (cfg.userName != null && !"".equals(cfg.userName.trim())
                && cfg.password != null && !"".equals(cfg.password.trim())) {
            b = b.withAuthProvider(new PlainTextAuthProvider(cfg.userName, cfg.password));
        } else {
            logger.info("No auth will be used. Either credentials are not provided or are incorrect.");
        }


        try (
                Cluster c = b.build();
                Session s = c.connect(keyspace)
        ) {

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


            generateModelCode(beans, tables, partitionKeys);
            generateAccessCode(s);

            if ("y".equalsIgnoreCase(cfg.toString)) {
                toStringMethods();
            }

            if ("y".equalsIgnoreCase(cfg.printInfo)) {
                info();
            }
            if ("y".equalsIgnoreCase(cfg.debug)) {
                model.build(new SingleStreamCodeWriter(System.out));
            } else {
                File source = new File(cfg.generatedSourceDir);
                if (source.exists() || source.mkdirs()) {
                    model.build(new File(cfg.generatedSourceDir));
                }
            }
        }

    }

    /**
     * Reads queries and metadata from .yml file and generates java data access layer based on cassandra schema
     * and obtained metadata.
     *
     * @param s cassandra session to use for query execution
     * @throws IOException thrown if .yml file does not exist or due t inability to read it
     */
    private void generateAccessCode(Session s) throws IOException {
        if (cfg.cqlFile != null && !"".equals(cfg.cqlFile.trim())) {
            File cql = new File(cfg.cqlFile);
            if (!cql.exists() || !cql.isFile()) {
                throw new IOException(
                        String.format(
                                "CQL file specified '%s' either " +
                                        "does not exist or is not a file.", cfg.cqlFile));

            }
            try (InputStream is = new FileInputStream(cql)) {
                Yaml yaml = new Yaml();
                Iterable<Object> it = yaml.loadAll(is);
                try {
                    JDefinedClass dao = model._class(JMod.PUBLIC, cfg.jpackage + "." + camelize(cfg.keysapce) + "DAO", ClassType.CLASS);
                    JFieldVar session_ = dao.field(JMod.FINAL | JMod.PRIVATE, Session.class, "session");
                    JMethod constructor = dao.constructor(JMod.PUBLIC);
                    JVar param = constructor.param(Session.class, "session");
                    constructor.body().assign(JExpr._this().ref(session_), param);
                    for (Object data : it) {
                        if (data instanceof LinkedHashMap) {
                            LinkedHashMap<String, String> map = (LinkedHashMap<String, String>) data;
                            if (!map.isEmpty()) {
                                String name = map.entrySet().iterator().next().getKey();
                                String cqlStatement = map.entrySet().iterator().next().getValue();

                                if (!"schema".equalsIgnoreCase(name)) {
                                    PreparedStatement ps = s.prepare(cqlStatement);
                                    ColumnDefinitions ds = ps.getVariables();
                                    PreparedId meta = ps.getPreparedId();
                                    // Due to some reason datastax does not want to expose metadata the same way as JDBC does
                                    // See - https://datastax-oss.atlassian.net/browse/JAVA-195
                                    // Ok, reflection then
                                    ColumnDefinitions metadata = null;
                                    ColumnDefinitions resultSetMetadata = null;
                                    try {
                                        Class<?> c = PreparedId.class;
                                        Field metadata_F = c.getDeclaredField("metadata");
                                        metadata_F.setAccessible(true);
                                        Object o = metadata_F.get(meta);
                                        if (o instanceof ColumnDefinitions) {
                                            metadata = (ColumnDefinitions) o;
                                        } else {
                                            throw new Exception("metadata is not an instance of ColumnDefinitions");
                                        }

                                        Field metadata_RS = c.getDeclaredField("resultSetMetadata");
                                        metadata_RS.setAccessible(true);
                                        o = metadata_RS.get(meta);
                                        if (o instanceof ColumnDefinitions) {
                                            resultSetMetadata = (ColumnDefinitions) o;
                                        }

                                    } catch (Exception e) {
                                        throw new RuntimeException("Failed to access metadata or resultsetMetaData of prepared statement.", e);
                                    }
                                    String table = resultSetMetadata == null ? metadata.getTable(0) : resultSetMetadata.getTable(0);
                                    JMethod method = dao.method(JMod.PUBLIC, model.ref(getFullCallName(table)), camelize(name, true));
                                    for (ColumnDefinitions.Definition cd : ds) {
                                        method.param(cd.getType().asJavaClass(), camelize(cd.getName(), true));
                                    }
                                    method.body()._return(JExpr._null());
                                }
                            }
                        }
                    }
                } catch (JClassAlreadyExistsException e) {
                    throw new RuntimeException("Failed to generate Data Access Object", e);
                }

            }

        }
    }

    /**
     * Generates java model (pojos) from existing cassandra CQL schema.
     *
     * @param beans         udt definitions
     * @param tables        table definitions
     * @param partitionKeys partition keys from table metadata
     */
    private void generateModelCode(
            Multimap<String, Pair<String, DataType>> beans,
            Multimap<String, Pair<String, ColumnMetadata>> tables,
            ArrayListMultimap<String, String> partitionKeys) {
        final JDefinedClass rowMapper;
        try {
            rowMapper = model._class(JMod.PUBLIC, cfg.jpackage + ".RowMapper", ClassType.INTERFACE);
            JTypeVar jtv = rowMapper.generify("T");
            rowMapper.method(JMod.NONE, jtv, "map").param(com.datastax.driver.core.GettableData.class, "data");
        } catch (Exception e) {
            throw new RuntimeException("Failed to generate mapper interface.", e);
        }

        for (String cl : beans.keySet()) {
            try {
                JDefinedClass clazz = JCQLUtils.getBeanClass(cfg.jpackage, camelize(cl), model);

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
                JDefinedClass clazz = JCQLUtils.getBeanClass(cfg.jpackage, camelize(table), model);
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
    }

    /**
     * Generates private field, getter and setter, some cassandra annotations
     *
     * @param clazz pojo definition
     * @param dt    data type of the field to be generated
     * @param name  name of the filed
     * @param pko   order of partition key if composite
     * @param ann   either @Field or @Column datastax annotation
     *              depending on whether table or udt is processed by method
     */
    private void javaBeanFieldWithGetterSetter(
            JDefinedClass clazz,
            DataType dt, String name, int pko,
            Class<? extends Annotation> ann) {
        JClass ref = getType(dt);
        JFieldVar f = clazz.field(JMod.PRIVATE, ref, camelize(name, true));
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
        clazz.method(JMod.PUBLIC, ref, "get" + camelize(name)).body()._return(JExpr._this().ref(f));
        JMethod m = clazz.method(JMod.PUBLIC, Void.TYPE, "set" + camelize(name));
        JVar p = m.param(ref, camelize(name, true));
        m.body().assign(JExpr._this().ref(f), p);
    }

    /**
     * Generates row mapper code for a specified pojo
     *
     * @param clazz     pojo class
     * @param rowMapper RowMapper interface
     * @param fields    fields to map
     * @throws JClassAlreadyExistsException thrown if class already exists in code model
     */
    private void rowMapperCode(JDefinedClass clazz, JClass rowMapper, Collection<Pair<String, DataType>> fields) throws JClassAlreadyExistsException {
        JClass rowMapperNarrowed = rowMapper.narrow(clazz);
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
                JBlock jb = body._if(JOp.not(param.invoke("isNull")
                        .arg(JExpr.lit(name))))._then();
                List<DataType> typeArgs = type.getTypeArguments();
                JExpression setterExpression;

                if (typeArgs.size() == 1) {
                    DataType arg = typeArgs.get(0);
                    JClass cl = getType(arg);
                    JExpression tclass = arg.isFrozen() ?
                            arg.asJavaClass().isAssignableFrom(UDTValue.class) ?
                                    model.ref(UDTValue.class).dotclass() : model.ref(TupleValue.class).dotclass()
                            : cl.dotclass();
                    setterExpression = param.invoke(getDataMethod(type.getName()))
                            .arg(name)
                            .arg(tclass);
                    if (arg.isFrozen()) {
                        JClass collectionClass = model.ref(
                                type.asJavaClass().isAssignableFrom(List.class) ?
                                        LinkedList.class : HashSet.class).narrow(cl);
                        JVar collection = jb.decl(collectionClass, camelize(name, true), JExpr._new(collectionClass));
                        JClass udtValueCollection = model.ref(type.asJavaClass()).narrow(UDTValue.class);
                        JVar udtCollection = jb.decl(udtValueCollection, camelize(name, true) + "Source", setterExpression);
                        // TODO handle tuples
                        JForEach forEach = jb.forEach(model.ref(UDTValue.class), "entry", udtCollection);
                        JVar var = forEach.var();
                        JBlock forEachBody = forEach.body();
                        forEachBody.add(collection.invoke("add").arg(cl.staticInvoke("mapper").invoke("map").arg(var)));
                        setterExpression = collection;
                    }
                } else if (typeArgs.size() == 2) {
                    DataType arg0 = typeArgs.get(0);
                    DataType arg1 = typeArgs.get(1);
                    JClass argc0 = getType(arg0);
                    JClass argc1 = getType(arg1);
                    if (arg0.isFrozen() || arg1.isFrozen()) {

                        JVar hashmap = jb.decl(model.ref(Map.class).narrow(argc0).narrow(argc1)
                                , camelize(name, true), JExpr._new(model.ref(HashMap.class)
                                .narrow(argc0).narrow(argc1)));
                        JExpression arg0csClass = model.ref(arg0.asJavaClass()).dotclass();
                        JExpression arg1csClass = model.ref(arg1.asJavaClass()).dotclass();

                        JVar frozen = jb.decl(model.ref(Map.class).narrow(arg0.asJavaClass(),
                                        arg1.asJavaClass()), camelize(name, true) + "Source",
                                param.invoke(getDataMethod(type.getName()))
                                        .arg(name)
                                        .arg(arg0csClass)
                                        .arg(arg1csClass));
                        JForEach forEach = jb.forEach(model.ref(Map.Entry.class).narrow(arg0.asJavaClass(),
                                arg1.asJavaClass()), "entry", frozen.invoke("entrySet"));
                        JVar var = forEach.var();
                        JBlock forEachBody = forEach.body();

                        forEachBody.add(hashmap.invoke("put")
                                        .arg(arg0.isFrozen() ?
                                                        argc0.staticInvoke("mapper").invoke("map").arg(var.invoke("getKey")) :
                                                        var.invoke("getKey")
                                        ).arg(arg1.isFrozen() ?
                                                argc1.staticInvoke("mapper").invoke("map").arg(var.invoke("getValue")) :
                                                var.invoke("getValue"))
                        );
                        setterExpression = hashmap;
                    } else {
                        setterExpression = param.invoke(getDataMethod(type.getName()))
                                .arg(name)
                                .arg(argc0.dotclass()).arg(argc1.dotclass());
                    }
                } else {
                    throw new RuntimeException(String.format("Unsupported arguments count %d: ", typeArgs.size()));
                }


                jb.add(bean.invoke("set" + camelize(name))
                        .arg(setterExpression));
            } else if (type.isFrozen()) {
                if (type instanceof UserType) {
                    UserType ut = (UserType) type;
                    body.add(bean.invoke("set" + camelize(name))
                            .arg(mapUDT(name, ut, param, type)));
                } else if (type instanceof TupleType) {
                    TupleType tt = (TupleType) type;
                    JBlock cond = body._if(JOp.not(param.invoke("isNull")
                            .arg(JExpr.lit(name))))._then();
                    mapTuple(tt, cond, name, param, bean, type);

                }
            } else {
                body.add(bean.invoke("set" + camelize(name))
                        .arg(param.invoke(getDataMethod(type.getName())).arg(name)));
            }
        }
        body._return(JExpr.direct("entity"));
        clazz.method(JMod.PUBLIC | JMod.STATIC, rowMapperNarrowed, "mapper").body()._return(JExpr.direct("mapper"));
    }

    /**
     * Maps tuple cassandra type to java tuple as a part of RowMapper#map(GettableData data) call
     *
     * @param tt    cassandra tuple type
     * @param body  body to append code to
     * @param name  name of a filed of type tuple
     * @param param map method argument - raw casandra type
     * @param bean  bean to invoke setter on
     * @param type
     */
    private void mapTuple(TupleType tt, JBlock body, String name, JVar param, JVar bean, DataType type) {
        List<DataType> dt = tt.getComponentTypes();
        JClass dts[] = new JClass[dt.size()];
        for (int i = 0; i < dts.length; i++) {
            dts[i] = getType(dt.get(i));
        }
        JVar t = body.decl(model.ref(TupleValue.class), camelize(name, true), param.invoke(getDataMethod(type.getName())).arg(name));
        JConditional iffy = body._if(t.ne(JExpr._null()));
        JBlock ifbody = iffy._then();
        JInvocation tc = model.ref(getTupleClass(dts.length)).staticInvoke("with");
        for (int i = 0; i < dts.length; i++) {
            DataType cdt = dt.get(i);
            if (cdt.isFrozen()) {
                if (cdt instanceof UserType) {
                    UserType ut = (UserType) cdt;
                    tc = tc.arg(mapUDT(i + "", ut, t, cdt));
                } else if (cdt instanceof TupleType) {
                    TupleType tuple = (TupleType) cdt;
                    // TODO need to support nested tuples. Will do later. Passing Null for now.
                    tc = tc.arg(JExpr.cast(getType(tuple), JExpr._null()));
                }
            } else if (cdt.isCollection()) {
                // TODO need to support nested collections within tuples. Will do later. Passing Null for now.
                tc = tc.arg(JExpr.cast(getType(cdt), JExpr._null()));
            } else {
                tc = tc.arg(t.invoke(getDataMethod(dt.get(i).getName())).arg(JExpr.lit(i)));
            }
        }
        ifbody.add(bean.invoke("set" + camelize(name)).arg(tc));
    }

    private JInvocation mapUDT(String name, UserType ut, JVar param, DataType type) {
        return model.ref(getFullCallName(ut.getTypeName())).staticInvoke("mapper").invoke("map")
                .arg(param.invoke(getDataMethod(type.getName()))
                        // somewhat very fragile and extremely straightforward but will stick with it form now
                        .arg(isInteger(name) ? JExpr.lit(Integer.valueOf(name)) : JExpr.lit(name)));
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
                // -- seems like datastax client doesn't handle tuples - dealing with it
                TupleType tt = (TupleType) t;
                List<DataType> dt = tt.getComponentTypes();
                JClass dts[] = new JClass[dt.size()];
                for (int i = 0; i < dts.length; i++) {
                    dts[i] = getType(dt.get(i));
                }
                return model.ref(getTupleClass(dts.length)).narrow(dts);
            }

        }
        return model.ref(t.asJavaClass());
    }

    private String getFullCallName(String name) {
        return cfg.jpackage + "." + camelize(name);
    }

    private void info() {
        logger.info("==================================================================");
        logger.info("Code Model Info :");
        logger.info("Artifacts Count : {}", model.countArtifacts());
        logger.info("Packages : ");
        Iterator<JPackage> it = model.packages();
        while (it.hasNext()) {
            JPackage jp;
            logger.info((jp = it.next()).name());
            Iterator<JDefinedClass> classIterator = jp.classes();
            while (classIterator.hasNext())
                logger.info("'- " + classIterator.next().fullName());
        }
        logger.info("==================================================================");
    }


    private void toStringMethods() {
        Iterator<JPackage> jPackageIterator = model.packages();
        while (jPackageIterator.hasNext()) {
            Iterator<JDefinedClass> jDefinedClassIterator = jPackageIterator.next().classes();
            while (jDefinedClassIterator.hasNext()) {
                JDefinedClass jdc = jDefinedClassIterator.next();
                if (jdc.isClass() && !jdc.isInterface()) {
                    JMethod jMethod = jdc.method(JMod.PUBLIC, model.ref(String.class), "toString");
                    JBlock body = jMethod.body();
                    JVar sb = body.decl(
                            JMod.FINAL, model.ref(StringBuilder.class),
                            "sb", JExpr._new(model.ref(StringBuilder.class))
                    );
                    body.add(sb.invoke("append").arg("{").invoke("append").arg(jdc.name()).invoke("append").arg(":{"));
                    int size = jdc.fields().size();
                    for (Map.Entry<String, JFieldVar> f : jdc.fields().entrySet()) {
                        if (!f.getValue().type().fullName().contains("RowMapper")) {
                            body.add(sb.invoke("append").arg(f.getKey())
                                    .invoke("append").arg("=")
                                    .invoke("append").arg(f.getValue()));
                            if (size != 1) {
                                body.add(sb.invoke("append").arg(","));
                            }
                        }
                        size--;
                    }
                    body.add(sb.invoke("append").arg("}}"));
                    body._return(sb.invoke("toString"));
                }
            }
        }
    }

}
