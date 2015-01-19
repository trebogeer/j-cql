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


import com.datastax.driver.core.BoundStatement;
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
import com.sun.codemodel.*;
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
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.sun.codemodel.ClassType.CLASS;
import static com.sun.codemodel.ClassType.INTERFACE;
import static com.sun.codemodel.JExpr.lit;
import static com.sun.codemodel.JMod.NONE;
import static com.sun.codemodel.JMod.PUBLIC;
import static com.trebogeer.jcql.JCQLUtils.camelize;
import static com.trebogeer.jcql.JCQLUtils.getDataMethod;
import static com.trebogeer.jcql.JCQLUtils.getFullClassName;
import static com.trebogeer.jcql.JCQLUtils.getTupleClass;
import static com.trebogeer.jcql.JCQLUtils.getType;
import static com.trebogeer.jcql.JCQLUtils.isInteger;
import static com.trebogeer.jcql.JCQLUtils.setDataMethod;

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
                Set<Pair<String, ColumnMetadata>> fields = new HashSet<>();
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
                    JDefinedClass dao = model._class(PUBLIC, cfg.jpackage + "." + camelize(cfg.keysapce) + "DAO", CLASS);
                    JFieldVar session_ = dao.field(JMod.FINAL | JMod.PRIVATE, Session.class, "session");
                    JMethod constructor = dao.constructor(PUBLIC);
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
                                    JMethod method = dao.method(PUBLIC, model.ref(getFullClassName(cfg.jpackage, table)), camelize(name, true));
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
     * // TODO - segregate mappers from pojos and make them separately configurable via options. The  whole stack of generated code might not always be needed.
     *
     * @param beans         udt definitions
     * @param tables        table definitions
     * @param partitionKeys partition keys from table metadata
     */
    private void generateModelCode(
            Multimap<String, Pair<String, DataType>> beans,
            Multimap<String, Pair<String, ColumnMetadata>> tables,
            ArrayListMultimap<String, String> partitionKeys) {
        JDefinedClass rowMapper;
        JDefinedClass toUDTMapper = null;
        JDefinedClass binder = null;
        try {
            rowMapper = model._class(PUBLIC, cfg.jpackage + ".RowMapper", INTERFACE);
            JTypeVar jtv = rowMapper.generify("T");
            rowMapper.method(NONE, jtv, "map").param(com.datastax.driver.core.GettableData.class, "data");
        } catch (Exception e) {
            throw new RuntimeException("Failed to generate mapper interface.", e);
        }

        if (tables != null && !tables.isEmpty()) {
            try {
                binder = model._class(PUBLIC, cfg.jpackage + ".TableBindMapper", INTERFACE);
                JTypeVar jtv = binder.generify("T");
                JMethod jm = binder.method(NONE, model.VOID, "bind");
                jm.param(jtv, "data");
                jm.param(model.ref(BoundStatement.class), "st");
                jm.param(model.ref(Session.class), "session");
            } catch (Exception e) {
                throw new RuntimeException("Failed to generate table bind interface.", e);
            }
        }

        if (beans != null && beans.size() > 0) {
            try {
                toUDTMapper = model._class(PUBLIC, cfg.jpackage + ".BeanToUDTMapper", INTERFACE);
                JTypeVar jtv = toUDTMapper.generify("T");
                JMethod toUDT = toUDTMapper.method(NONE, model.ref(UDTValue.class), "toUDT");
                JVar toUDTArg0 = toUDT.param(jtv, "data");
                JVar toUDTArg1 = toUDT.param(Session.class, "session");
            } catch (JClassAlreadyExistsException e) {
                throw new RuntimeException("Failed to generate UDT mapper interface.", e);
            }
        }
        if (beans != null) {
            for (String cl : beans.keySet()) {
                try {
                    JDefinedClass clazz = JCQLUtils.getBeanClass(cfg.jpackage, camelize(cl), model);

                    // row mapper
                    rowMapperCode(clazz, rowMapper, beans.get(cl));

                    // pojo to UDT mapper
                    toUDTMapperCode(clazz, toUDTMapper, beans.get(cl), cl);

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
        }
        if (tables != null && !tables.isEmpty()) {
            for (String table : tables.keySet()) {
                try {
                    JDefinedClass clazz = JCQLUtils.getBeanClass(cfg.jpackage, camelize(table), model);


                    Collection<Pair<String, DataType>> dataTypes = Collections2.transform(tables.get(table), new Function<Pair<String, ColumnMetadata>, Pair<String, DataType>>() {
                        @Override
                        public Pair<String, DataType> apply(Pair<String, ColumnMetadata> input) {
                            return Pair.with(input.getValue0(), input.getValue1().getType());
                        }
                    });

                    // row mapper
                    rowMapperCode(clazz, rowMapper, dataTypes);

                    // bind to statement code

                    binderToStatemet(clazz, binder, dataTypes);

                    // fields/getters/setters/annotations
                    clazz.annotate(Table.class).param("keyspace", cfg.keysapce).param("name", table);
                    List<String> pkList = partitionKeys.get(table);
                    Set<String> pks = new HashSet<>(pkList);

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
    }


    /**
     * Binds POJO to BoundStatement basing on PreparedStatement metadata by
     * introspecting ColumnDefinitions and invoking BoundStatement#bind(...)
     *
     * @param clazz     pojo class
     * @param binder    binder interface
     * @param dataTypes collection of names and DataTypes of pojo fields
     */
    private void binderToStatemet(
            JDefinedClass clazz, JDefinedClass binder,
            Collection<Pair<String, DataType>> dataTypes
    ) throws JClassAlreadyExistsException {
        JClass bindMapperNarrowed = binder.narrow(clazz);
        JDefinedClass bindImpl = clazz._class(
                JMod.FINAL
                        | JMod.STATIC | JMod.PRIVATE, clazz.name() + "BindMapper")
                ._implements(bindMapperNarrowed);
        JVar bindSt = clazz.field(JMod.PRIVATE | JMod.STATIC | JMod.FINAL, bindImpl, "bind", JExpr._new(bindImpl));
        clazz.method(PUBLIC | JMod.STATIC, bindImpl, "bind").body()._return(bindSt);

        JMethod bind = bindImpl.method(PUBLIC, model.VOID, "bind");
        JVar dataBind = bind.param(clazz, "data");
        JVar st = bind.param(BoundStatement.class, "st");
        JVar session = bind.param(Session.class, "session");
        JBlock body = bind.body();

        body._if(dataBind.eq(JExpr._null()))._then()._return();
        body._if(st.eq(JExpr._null()))._then()._throw(JExpr._new(model.ref(IllegalArgumentException.class))
                .arg("Cassandra BoundStatement can't be null."));
        JVar defsMap = body.decl(model.ref(Map.class).narrow(String.class, Integer.class),
                "defSet", JExpr._new(model.ref(HashMap.class).narrow(String.class, Integer.class)));
        JVar columnDefinitions = body.decl(model.ref(ColumnDefinitions.class), "cds",
                st.invoke("preparedStatement").invoke("getVariables"));
        JVar cnt = body.decl(model.INT, "cnt", JExpr.lit(0));
        JForEach forEach0 = body.forEach(model.ref(ColumnDefinitions.Definition.class),
                "entry", columnDefinitions.invoke("asList"));
        JVar entry0 = forEach0.var();
        JBlock forEachBody0 = forEach0.body();
        forEachBody0.add(defsMap.invoke("put").arg(entry0.invoke("getName")).arg(cnt));
        forEachBody0.assignPlus(cnt, JExpr.lit(1));
        
        JVar bindArgs = body.decl(model.ref(Object[].class), "bindArgs",
                JExpr.newArray(model.ref(Object.class), defsMap.invoke("size")));

        for (Pair<String, DataType> field : dataTypes) {
            DataType dt = field.getValue1();
            String fname = field.getValue0();
            String fnamec = camelize(fname);
            String fnamecl = camelize(fname, true);
            JExpression arg2 = JExpr._null();
            JBlock ifBody = body._if(defsMap.invoke("containsKey").arg(JExpr.lit(fname)))._then();
            

        }
    }



        /**
         * Maps pojo to UDT for subsequent update/insert.
         *
         * @param clazz     pojo class
         * @param udtMapper to udt mapper interface
         * @param fields    collection of pojo fields
         * @param name      name of user type in cassandra ddl
         * @throws JClassAlreadyExistsException thrown if mapper already exists
         */

    private void toUDTMapperCode(
            JDefinedClass clazz, JClass udtMapper,
            Collection<Pair<String, DataType>> fields,
            String name) throws JClassAlreadyExistsException {
        JClass udtMapperNarrowed = udtMapper.narrow(clazz);
        JDefinedClass mapperImpl = clazz._class(
                JMod.FINAL | JMod.STATIC | JMod.PRIVATE, clazz.name() + "ToUDTMapper")
                ._implements(udtMapperNarrowed);
        JVar udtMapperSt = clazz.field(JMod.PRIVATE | JMod.STATIC | JMod.FINAL, mapperImpl, "udt_mapper", JExpr._new(mapperImpl));
        clazz.method(PUBLIC | JMod.STATIC, udtMapperNarrowed, "udtMapper").body()._return(udtMapperSt);

        JMethod toUDT = mapperImpl.method(PUBLIC, model.ref(UDTValue.class), "toUDT");
        JVar dataUdt = toUDT.param(clazz, "data");
        JVar session = toUDT.param(Session.class, "session");
        JBlock body = toUDT.body();

        body._if(dataUdt.eq(JExpr._null()))._then()._return(JExpr._null());
        body._if(session.eq(JExpr._null()))._then()._throw(JExpr._new(model.ref(IllegalArgumentException.class)).arg("Cassandra Session can't be null."));

        JVar userType = body.decl(model.ref(UserType.class), "userType",
                session.invoke("getCluster")
                        .invoke("getMetadata")
                        .invoke("getKeyspace").arg(lit(cfg.keysapce))
                        .invoke("getUserType").arg(lit(name))
        );
        JVar udt = body.decl(model.ref(UDTValue.class), "udtValue", userType.invoke("newValue"));

        for (Pair<String, DataType> field : fields) {
            DataType dt = field.getValue1();
            String fname = field.getValue0();
            String fnamec = camelize(fname);
            String fnamecl = camelize(fname, true);
            JExpression arg2 = JExpr._null();
            if (dt.isFrozen()) {

                if (dt instanceof UserType) {
                    arg2 = model.ref(getFullClassName(cfg.jpackage, ((UserType) dt).getTypeName()))
                            .staticInvoke("udtMapper")
                            .invoke("toUDT").arg(dataUdt.invoke("get" + fnamec)).arg(session);
                } else if (dt instanceof TupleType) {
                    TupleType tt = (TupleType) dt;
                    List<DataType> componentTypes = tt.getComponentTypes();
                    Class<?> tupleClass = getTupleClass(componentTypes.size());
                    JClass tuple = model.ref(tupleClass);

                    JInvocation of = model.ref(TupleType.class).staticInvoke("of");
                    for (DataType adt : componentTypes) {
                        tuple = tuple.narrow(getType(adt, model, cfg));
                    }
                    JVar tupleRef = body.decl(tuple, fnamecl + "Tuple", dataUdt.invoke("get" + fnamec));

                    int i = 0;
                    ArrayList<JExpression> getvalues = new ArrayList<>(componentTypes.size());
                    for (DataType adt : componentTypes) {
                        JExpression jexpr = JExpr._null();
                        getvalues.add(i, tupleRef.invoke("getValue" + i));
                        if (adt.isFrozen()) {
                            if (adt instanceof UserType) {
                                UserType ut = (UserType) adt;
                                String tname = ut.getTypeName();

                                JVar udtValue = body.decl(model.ref(UDTValue.class),
                                        camelize(tname, true),
                                        model.ref(getFullClassName(cfg.jpackage, tname))
                                                .staticInvoke("udtMapper").invoke("toUDT")
                                                .arg(tupleRef.invoke("getValue" + i)).arg(session));

                                JVar userTypeE = body.decl(model.ref(UserType.class), camelize(tname, true) + "UserType",
                                        udtValue.invoke("getType"));
                                jexpr = userTypeE;
                                getvalues.set(i, udtValue);
                            } else if (adt instanceof TupleType) {
                                // TODO yep, all the hell above once again
                            }
                        } else if (adt.isCollection()) {

                        } else {
                            jexpr = model.ref(DataType.class).staticInvoke(adt.getName().name().toLowerCase());
                        }
                        i++;
                        of.arg(jexpr);
                    }
                    JVar ttE = body.decl(model.ref(TupleType.class), camelize(fname, true) + "TupleType", of);
                    JVar tvE = body.decl(model.ref(TupleValue.class), camelize(fname, true) + "TupleValue", ttE.invoke("newValue"));
                    for (int a = 0; a < getvalues.size(); a++) {
                        body.add(tvE.invoke(setDataMethod(componentTypes.get(a).getName())).arg(JExpr.lit(a)).arg(getvalues.get(a)));
                    }
                    arg2 = tvE;
                }
            } else if (dt.isCollection()) {
                List<DataType> argTypes = dt.getTypeArguments();
                if (argTypes != null) {
                    if (argTypes.size() == 1) {
                        DataType argDt = argTypes.get(0);
                        if (argDt.isCollection()) {
                            // TODO cassandra does not support embedded collections yet but might support in future
                            throw new UnsupportedOperationException("Collections of collections are not" +
                                    " supported within UDTs and probably by cassandra 2.1.");
                        } else if (argDt.isFrozen()) {
                            if (argDt instanceof UserType) {
                                UserType ut = (UserType) argDt;
                                String utname = ut.getTypeName();
                                String utnamec = camelize(utname);
                                JClass sourceCollectionGeneric = model.ref(getFullClassName(cfg.jpackage, utnamec));
                                JClass sourceCollectionClass = model.ref(dt.asJavaClass())
                                        .narrow(sourceCollectionGeneric);

                                JVar source = body.decl(sourceCollectionClass, fnamecl + "Source",
                                        dataUdt.invoke("get" + camelize(fname)));
                                JVar target = body.decl(model.ref(dt.asJavaClass()).narrow(UDTValue.class),
                                        fnamecl + "Target",
                                        JExpr._new(dt.asJavaClass().isAssignableFrom(Set.class)
                                                ? model.ref(HashSet.class).narrow(UDTValue.class)
                                                : model.ref(LinkedList.class).narrow(UDTValue.class)));
                                JForEach forEach = body.forEach(sourceCollectionGeneric, "entry", source);
                                JVar entry = forEach.var();
                                JBlock forEachBody = forEach.body();

                                JInvocation convertToUDT = sourceCollectionGeneric
                                        .staticInvoke("udtMapper").invoke("toUDT").arg(entry).arg(session);
                                forEachBody.add(target.invoke("add").arg(convertToUDT));
                                arg2 = target;
                            } else if (argDt instanceof TupleType) {
                                // TODO support tuples
                                throw new UnsupportedOperationException("Collections of tuples within " +
                                        "UDT are not yet supported.");
                            }
                        } else {
                            arg2 = dataUdt.invoke("get" + camelize(fname));
                        }
                    } else if (argTypes.size() == 2) {
                        DataType argDt0 = argTypes.get(0);
                        DataType argDt1 = argTypes.get(1);
                        JClass argc0 = getType(argDt0, model, cfg);
                        JClass argc1 = getType(argDt1, model, cfg);
                        if (argDt0.isCollection() || argDt1.isCollection()) {
                            // TODO cassandra does not support embedded collections yet but might support in future
                            throw new UnsupportedOperationException("Collections of collections are not" +
                                    " supported within UDTs and probably by cassandra 2.1.");
                        } else if (argDt0.isFrozen() || argDt1.isFrozen()) {
                            if (argDt0 instanceof TupleType || argDt1 instanceof TupleType) {
                                throw new UnsupportedOperationException("Collections of tuples are not yet" +
                                        " supported within UDTs.");
                            }

                            JVar source = body.decl(model.ref(Map.class).narrow(argc0, argc1),
                                    fnamecl + "Source", dataUdt.invoke("get" + camelize(fname)));
                            JVar target = body.decl(model.ref(Map.class).narrow(argDt0.asJavaClass(), argDt1.asJavaClass()),
                                    fnamecl + "Target",
                                    JExpr._new(model.ref(HashMap.class).narrow(argDt0.asJavaClass(), argDt1.asJavaClass())));
                            JClass entryClass = model.ref(Map.Entry.class).narrow(argc0, argc1);
                            JForEach forEach = body.forEach(entryClass, "entry", source.invoke("entrySet"));
                            JVar entry = forEach.var();
                            JExpression k = entry.invoke("getKey");
                            JExpression v = entry.invoke("getValue");
                            JBlock forEachBody = forEach.body();
                            JExpression key = argDt0.isFrozen()
                                    ? argc0.staticInvoke("udtMapper").invoke("toUDT").arg(k).arg(session)
                                    : k;
                            JExpression value = argDt1.isFrozen()
                                    ? argc1.staticInvoke("udtMapper").invoke("toUDT").arg(v).arg(session)
                                    : v;
                            forEachBody.add(target.invoke("put").arg(key).arg(value));
                            arg2 = target;
                        } else {
                            arg2 = dataUdt.invoke("get" + camelize(fname));
                        }
                    }
                }
            } else {
                arg2 = dataUdt.invoke("get" + camelize(fname));
            }
            body.add(udt.invoke(setDataMethod(dt.getName())).arg(lit(fname)).arg(arg2));

        }

        body._return(udt);

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
        JClass ref = getType(dt, model, cfg);
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
        clazz.method(PUBLIC, ref, "get" + camelize(name)).body()._return(JExpr._this().ref(f));
        JMethod m = clazz.method(PUBLIC, Void.TYPE, "set" + camelize(name));
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

        JMethod map = mapperImpl.method(PUBLIC, clazz, "map");
        JVar param = map.param(com.datastax.driver.core.GettableData.class, "data");
        JBlock body = map.body();
        body._if(param.eq(JExpr._null()))._then()._return(JExpr._null());
        JVar bean = body.decl(clazz, "entity", JExpr._new(clazz));
        for (Pair<String, DataType> field : fields) {
            String name = field.getValue0();
            DataType type = field.getValue1();
            if (type.isCollection()) {
                JBlock jb = body._if(JOp.not(param.invoke("isNull")
                        .arg(lit(name))))._then();
                List<DataType> typeArgs = type.getTypeArguments();
                JExpression setterExpression;

                if (typeArgs.size() == 1) {
                    DataType arg = typeArgs.get(0);
                    JClass cl = getType(arg, model, cfg);
                    JExpression tclass = arg.isFrozen() ?
                            arg.asJavaClass().isAssignableFrom(UDTValue.class) ?
                                    model.ref(UDTValue.class).dotclass() : model.ref(TupleValue.class).dotclass()
                            : cl.dotclass();
                    setterExpression = param.invoke(getDataMethod(type.getName()))
                            .arg(name)
                            .arg(tclass);
                    if (arg.isFrozen()) {
                        if (arg instanceof UserType) {
                            JClass collectionClass = model.ref(
                                    type.asJavaClass().isAssignableFrom(List.class) ?
                                            LinkedList.class : HashSet.class).narrow(cl);
                            JVar collection = jb.decl(collectionClass, camelize(name, true), JExpr._new(collectionClass));
                            JClass udtValueCollection = model.ref(type.asJavaClass()).narrow(UDTValue.class);
                            JVar udtCollection = jb.decl(udtValueCollection, camelize(name, true) + "Source", setterExpression);

                            JForEach forEach = jb.forEach(model.ref(UDTValue.class), "entry", udtCollection);
                            JVar var = forEach.var();
                            JBlock forEachBody = forEach.body();
                            forEachBody.add(collection.invoke("add").arg(cl.staticInvoke("mapper").invoke("map").arg(var)));
                            setterExpression = collection;
                        } else if (arg instanceof TupleType) {
                            // TODO handle tuples
                            throw new UnsupportedOperationException("Collections of tuples" +
                                    " are not yet supported when converting from Row to POJO.");
                        }
                    }
                } else if (typeArgs.size() == 2) {
                    DataType arg0 = typeArgs.get(0);
                    DataType arg1 = typeArgs.get(1);
                    JClass argc0 = getType(arg0, model, cfg);
                    JClass argc1 = getType(arg1, model, cfg);
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
                            .arg(lit(name))))._then();
                    mapTuple(tt, cond, name, param, bean, type);

                }
            } else {
                body.add(bean.invoke("set" + camelize(name))
                        .arg(param.invoke(getDataMethod(type.getName())).arg(name)));
            }
        }
        body._return(JExpr.direct("entity"));
        clazz.method(PUBLIC | JMod.STATIC, rowMapperNarrowed, "mapper").body()._return(JExpr.direct("mapper"));
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
            dts[i] = getType(dt.get(i), model, cfg);
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
                    tc = tc.arg(JExpr.cast(getType(tuple, model, cfg), JExpr._null()));
                }
            } else if (cdt.isCollection()) {
                // TODO need to support nested collections within tuples. Will do later. Passing Null for now.
                tc = tc.arg(JExpr.cast(getType(cdt, model, cfg), JExpr._null()));
            } else {
                tc = tc.arg(t.invoke(getDataMethod(dt.get(i).getName())).arg(lit(i)));
            }
        }
        ifbody.add(bean.invoke("set" + camelize(name)).arg(tc));
    }

    private JInvocation mapUDT(String name, UserType ut, JVar param, DataType type) {
        return model.ref(getFullClassName(cfg.jpackage, ut.getTypeName())).staticInvoke("mapper").invoke("map")
                .arg(param.invoke(getDataMethod(type.getName()))
                        // somewhat very fragile and extremely straightforward but will stick with it form now
                        .arg(isInteger(name) ? lit(Integer.valueOf(name)) : lit(name)));
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
                    JMethod jMethod = jdc.method(PUBLIC, model.ref(String.class), "toString");
                    JBlock body = jMethod.body();
                    JVar sb = body.decl(
                            JMod.FINAL, model.ref(StringBuilder.class),
                            "sb", JExpr._new(model.ref(StringBuilder.class))
                    );
                    body.add(sb.invoke("append").arg("{").invoke("append").arg(jdc.name()).invoke("append").arg(":{"));
                    int size = jdc.fields().size();
                    for (Map.Entry<String, JFieldVar> f : jdc.fields().entrySet()) {
                        if (!f.getValue().type().fullName().contains("Mapper")) {
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
