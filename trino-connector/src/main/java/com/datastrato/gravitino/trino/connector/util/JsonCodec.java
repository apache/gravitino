/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.trino.connector.util;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.datatype.guava.GuavaModule;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.fasterxml.jackson.module.paramnames.ParameterNamesModule;
import io.airlift.json.RecordAutoDetectModule;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockEncodingSerde;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorInsertTableHandle;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeManager;
import io.trino.spi.type.TypeSignature;
import java.io.File;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.net.URL;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

public class JsonCodec {
  private static JsonCodec INSTANCE;
  private ObjectMapper mapper;

  public static synchronized JsonCodec instance(ClassLoader appClassLoader) {
    if (INSTANCE != null) {
      return INSTANCE;
    }
    INSTANCE = new JsonCodec(appClassLoader, null);
    return INSTANCE;
  }

  public static synchronized JsonCodec instance(
      ClassLoader appClassLoader, TypeManager typeManager) {
    INSTANCE = new JsonCodec(appClassLoader, typeManager);
    return INSTANCE;
  }

  public final Map<String, ClassLoader> classLoaders = new ConcurrentHashMap<>();

  public JsonCodec(ClassLoader appClassLoader, TypeManager typeManager) {

    createClassLoaders(appClassLoader);

    Function<Object, String> nameResolver =
        obj -> {
          try {
            ClassLoader pluginClassloader = obj.getClass().getClassLoader();
            Field idField = pluginClassloader.getClass().getDeclaredField("pluginName");
            idField.setAccessible(true);
            String id = (String) idField.get(pluginClassloader);
            return id + ":" + obj.getClass().getName();
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        };

    Function<String, Class<?>> classResolver =
        name -> {
          try {
            String[] nameParts = name.split(":");
            String id = nameParts[0];
            String className = nameParts[1];
            ClassLoader loader = classLoaders.get(id);
            return loader.loadClass(className);
          } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
          }
        };

    try {
      if (typeManager == null) {
        Class internalTypeManagerClass =
            appClassLoader.loadClass("io.trino.type.InternalTypeManager");
        Class typeManagerClass = appClassLoader.loadClass("io.trino.spi.type.TypeManager");
        Class typeRegistryClass = appClassLoader.loadClass("io.trino.metadata.TypeRegistry");
        Class typeOperatorsClass = appClassLoader.loadClass("io.trino.spi.type.TypeOperators");
        Class featuresConfigClass = appClassLoader.loadClass("io.trino.FeaturesConfig");
        Class internalBlockEncodingSerdeClass =
            appClassLoader.loadClass("io.trino.metadata.InternalBlockEncodingSerde");
        Class blockEncodingSerdeClass =
            appClassLoader.loadClass("io.trino.spi.block.BlockEncodingSerde");
        Class jsonPath2016TypeClass = appClassLoader.loadClass("io.trino.type.JsonPath2016Type");
        Class typeDeserializerClass = appClassLoader.loadClass("io.trino.type.TypeDeserializer");
        Class blockEncodingManagerClass =
            appClassLoader.loadClass("io.trino.metadata.BlockEncodingManager");
        Class typeClass = appClassLoader.loadClass("io.trino.spi.type.Type");

        Object typeRegistry =
            typeRegistryClass
                .getConstructor(typeOperatorsClass, featuresConfigClass)
                .newInstance(
                    typeOperatorsClass.getConstructor().newInstance(),
                    featuresConfigClass.getConstructor().newInstance());
        typeManager =
            (TypeManager)
                internalTypeManagerClass
                    .getConstructor(typeRegistryClass)
                    .newInstance(typeRegistry);

        /*
        Object jsonPath2016Type =
            jsonPath2016TypeClass
                .getConstructor(typeDeserializerClass, blockEncodingSerdeClass)
                .newInstance(
                    typeDeserializerClass.getConstructor(typeManagerClass).newInstance(typeManager),
                    internalBlockEncodingSerdeClass
                        .getConstructor(blockEncodingManagerClass, typeManagerClass)
                        .newInstance(
                            blockEncodingManagerClass.getConstructor().newInstance(), typeManager));
        typeRegistryClass.getMethod("addType", typeClass).invoke(typeRegistry, jsonPath2016Type);
         */
      }

      buildMapper(typeManager, nameResolver, classResolver);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  BlockEncodingSerde createBlockEncodingSerde(TypeManager typeManager) throws Exception {
    ClassLoader classLoader = typeManager.getClass().getClassLoader();
    Class blockEncodingManagerClass =
        classLoader.loadClass("io.trino.metadata.BlockEncodingManager");
    Class internalBlockEncodingSerdeClass =
        classLoader.loadClass("io.trino.metadata.InternalBlockEncodingSerde");
    return (BlockEncodingSerde)
        internalBlockEncodingSerdeClass
            .getConstructor(blockEncodingManagerClass, TypeManager.class)
            .newInstance(blockEncodingManagerClass.getConstructor().newInstance(), typeManager);
  }

  private void createClassLoaders(ClassLoader appClassLoader) {

    try {
      File directory = new File("/usr/local/trino-server/plugin/hive");
      List<URL> files =
          Arrays.stream(directory.listFiles())
              .map(File::toURI)
              .map(
                  uri -> {
                    try {
                      return uri.toURL();
                    } catch (Exception e) {
                      throw new RuntimeException(e);
                    }
                  })
              .toList();

      Class<?> pluginClass = null;

      pluginClass = appClassLoader.loadClass("io.trino.server.PluginClassLoader");

      Constructor<?> constructor =
          pluginClass.getConstructor(String.class, List.class, ClassLoader.class, List.class);
      Object classLoader =
          constructor.newInstance(
              "gravitino-hive",
              files,
              this.getClass().getClassLoader(),
              List.of(
                  "io.trino.spi.",
                  "com.fasterxml.jackson.annotation.",
                  "io.airlift.slice.",
                  "org.openjdk.jol.",
                  "io.opentelemetry.api.",
                  "io.opentelemetry.context."));

      classLoaders.put("gravitino-hive", (ClassLoader) classLoader);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private void buildMapper(
      TypeManager typeManger,
      Function<Object, String> nameResolver,
      Function<String, Class<?>> classResolver) {

    ObjectMapper objectMapper = new ObjectMapper();
    // ignore unknown fields (for backwards compatibility)
    objectMapper.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
    // do not allow converting a float to an integer
    objectMapper.disable(DeserializationFeature.ACCEPT_FLOAT_AS_INT);
    // use ISO dates
    objectMapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);

    // Skip fields that are null or absent (Optional) when serializing objects.
    // This only applies to mapped object fields, not containers like Map or List.
    objectMapper.setDefaultPropertyInclusion(
        JsonInclude.Value.construct(JsonInclude.Include.NON_ABSENT, JsonInclude.Include.ALWAYS));

    // disable auto detection of json properties... all properties must be explicit
    objectMapper.disable(MapperFeature.AUTO_DETECT_CREATORS);
    objectMapper.disable(MapperFeature.AUTO_DETECT_FIELDS);
    objectMapper.disable(MapperFeature.AUTO_DETECT_SETTERS);
    objectMapper.disable(MapperFeature.AUTO_DETECT_GETTERS);
    objectMapper.disable(MapperFeature.AUTO_DETECT_IS_GETTERS);
    objectMapper.disable(MapperFeature.USE_GETTERS_AS_SETTERS);
    objectMapper.disable(MapperFeature.CAN_OVERRIDE_ACCESS_MODIFIERS);
    objectMapper.disable(MapperFeature.INFER_PROPERTY_MUTATORS);
    objectMapper.disable(MapperFeature.ALLOW_FINAL_FIELDS_AS_MUTATORS);

    objectMapper.registerModule(
        new AbstractTypedJacksonModule<>(
            ConnectorTransactionHandle.class, nameResolver, classResolver) {});

    objectMapper.registerModule(
        new AbstractTypedJacksonModule<>(
            ConnectorTableHandle.class, nameResolver, classResolver) {});

    objectMapper.registerModule(
        new AbstractTypedJacksonModule<>(ColumnHandle.class, nameResolver, classResolver) {});

    objectMapper.registerModule(
        new AbstractTypedJacksonModule<>(ConnectorSplit.class, nameResolver, classResolver) {});

    objectMapper.registerModule(
        new AbstractTypedJacksonModule<>(
            ConnectorInsertTableHandle.class, nameResolver, classResolver) {});

    SimpleModule module = new SimpleModule();
    objectMapper.registerModule(new Jdk8Module());
    objectMapper.registerModule(new JavaTimeModule());
    objectMapper.registerModule(new GuavaModule());
    objectMapper.registerModule(new ParameterNamesModule());
    objectMapper.registerModule(new RecordAutoDetectModule());

    try {
      module.addDeserializer(Type.class, new TypeDeserializer(typeManger));
      module.addDeserializer(
          TypeSignature.class,
          new TypeSignatureDeserializer(typeManger.getClass().getClassLoader()));

      BlockEncodingSerde blockEncodingSerde = createBlockEncodingSerde(typeManger);
      module.addSerializer(Block.class, new BlockJsonSerde.Serializer(blockEncodingSerde));
      module.addDeserializer(Block.class, new BlockJsonSerde.Deserializer(blockEncodingSerde));
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    objectMapper.registerModule(module);
    mapper = objectMapper;
  }

  public ClassLoader getClassLoader(String name) {
    return classLoaders.get("gravitino-" + name);
  }

  public ObjectMapper getMapper() {
    return mapper;
  }
}
