/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.gravitino.trino.connector.util.json;

import static org.apache.gravitino.trino.connector.GravitinoErrorCode.GRAVITINO_RUNTIME_ERROR;

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
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockEncodingSerde;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorInsertTableHandle;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.type.StandardTypes;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeManager;
import io.trino.spi.type.TypeSignature;
import java.lang.reflect.Field;
import java.util.function.Function;
import org.apache.gravitino.trino.connector.GravitinoConnectorPluginManager;

/**
 * Utility class for JSON serialization and deserialization in the Gravitino Trino connector.
 * Provides functionality for handling various Trino-specific types and plugin classes.
 */
public class JsonCodec {
  private static volatile ObjectMapper mapper;
  private static volatile Type jsonType;

  private static ObjectMapper buildMapper(ClassLoader classLoader) {
    try {
      ClassLoader appClassLoader =
          GravitinoConnectorPluginManager.instance(classLoader).getAppClassloader();
      TypeManager typeManager = createTypeManager(appClassLoader);
      return createMapper(typeManager);
    } catch (Exception e) {
      throw new TrinoException(GRAVITINO_RUNTIME_ERROR, "Failed to build ObjectMapper", e);
    }
  }

  private static Type buildJsonType(ClassLoader classLoader) {
    try {
      ClassLoader appClassLoader =
          GravitinoConnectorPluginManager.instance(classLoader).getAppClassloader();
      TypeManager typeManager = createTypeManager(appClassLoader);
      return typeManager.getType(new TypeSignature(StandardTypes.JSON));
    } catch (Exception e) {
      throw new TrinoException(GRAVITINO_RUNTIME_ERROR, "Failed to build JsonType", e);
    }
  }

  static TypeManager createTypeManager(ClassLoader classLoader) {
    try {
      Class internalTypeManagerClass = classLoader.loadClass("io.trino.type.InternalTypeManager");
      Class typeRegistryClass = classLoader.loadClass("io.trino.metadata.TypeRegistry");
      Class typeOperatorsClass = classLoader.loadClass("io.trino.spi.type.TypeOperators");
      Class featuresConfigClass = classLoader.loadClass("io.trino.FeaturesConfig");

      Object typeRegistry =
          typeRegistryClass
              .getConstructor(typeOperatorsClass, featuresConfigClass)
              .newInstance(
                  typeOperatorsClass.getConstructor().newInstance(),
                  featuresConfigClass.getConstructor().newInstance());
      return (TypeManager)
          internalTypeManagerClass.getConstructor(typeRegistryClass).newInstance(typeRegistry);
    } catch (Exception e) {
      throw new RuntimeException("Failed to create TypeManager :" + e.getMessage(), e);
    }
  }

  static BlockEncodingSerde createBlockEncodingSerde(TypeManager typeManager) throws Exception {
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

  static void registerHandleSerializationModule(ObjectMapper objectMapper) {

    GravitinoConnectorPluginManager pluginManager = GravitinoConnectorPluginManager.instance();
    Function<Object, String> nameResolver =
        obj -> {
          try {
            ClassLoader pluginClassloader = obj.getClass().getClassLoader();
            if (pluginClassloader
                == GravitinoConnectorPluginManager.instance().getAppClassloader()) {
              return "app:" + obj.getClass().getName();
            }

            Field idField = pluginClassloader.getClass().getDeclaredField("pluginName");
            idField.setAccessible(true);
            String classLoaderName = (String) idField.get(pluginClassloader);
            return classLoaderName + ":" + obj.getClass().getName();
          } catch (Exception e) {
            throw new RuntimeException("Gravitino connector serialize error " + e.getMessage(), e);
          }
        };

    Function<String, Class<?>> classResolver =
        name -> {
          try {
            String[] nameParts = name.split(":");
            String classLoaderName = nameParts[0];
            String className = nameParts[1];
            ClassLoader loader = pluginManager.getClassLoader(classLoaderName);
            return loader.loadClass(className);
          } catch (ClassNotFoundException e) {
            throw new RuntimeException(
                "Gravitino connector deserialize error " + e.getMessage(), e);
          }
        };

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
  }

  @SuppressWarnings("deprecation")
  private static ObjectMapper createMapper(TypeManager typeManager) {
    try {
      ObjectMapper objectMapper = new ObjectMapper();

      objectMapper.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
      objectMapper.disable(DeserializationFeature.ACCEPT_FLOAT_AS_INT);
      objectMapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
      objectMapper.setDefaultPropertyInclusion(
          JsonInclude.Value.construct(JsonInclude.Include.NON_ABSENT, JsonInclude.Include.ALWAYS));

      objectMapper.disable(MapperFeature.AUTO_DETECT_CREATORS);
      objectMapper.disable(MapperFeature.AUTO_DETECT_FIELDS);
      objectMapper.disable(MapperFeature.AUTO_DETECT_SETTERS);
      objectMapper.disable(MapperFeature.AUTO_DETECT_GETTERS);
      objectMapper.disable(MapperFeature.AUTO_DETECT_IS_GETTERS);
      objectMapper.disable(MapperFeature.USE_GETTERS_AS_SETTERS);
      objectMapper.disable(MapperFeature.CAN_OVERRIDE_ACCESS_MODIFIERS);
      objectMapper.disable(MapperFeature.INFER_PROPERTY_MUTATORS);
      objectMapper.disable(MapperFeature.ALLOW_FINAL_FIELDS_AS_MUTATORS);

      SimpleModule module = new SimpleModule();
      objectMapper.registerModule(new Jdk8Module());
      objectMapper.registerModule(new JavaTimeModule());
      objectMapper.registerModule(new GuavaModule());
      objectMapper.registerModule(new ParameterNamesModule());
      objectMapper.registerModule(new RecordAutoDetectModule());

      // Handle serialization for plugin classes
      registerHandleSerializationModule(objectMapper);

      // Type serialization for plugin classes
      module.addDeserializer(Type.class, new TypeDeserializer(typeManager));
      module.addDeserializer(
          TypeSignature.class,
          new TypeSignatureDeserializer(typeManager.getClass().getClassLoader()));

      // Block serialization for plugin classes
      BlockEncodingSerde blockEncodingSerde = createBlockEncodingSerde(typeManager);
      module.addSerializer(Block.class, new BlockJsonSerde.Serializer(blockEncodingSerde));
      module.addDeserializer(Block.class, new BlockJsonSerde.Deserializer(blockEncodingSerde));

      objectMapper.registerModule(module);
      return objectMapper;
    } catch (Exception e) {
      throw new RuntimeException("Failed to create JsonMapper:" + e.getMessage(), e);
    }
  }

  /**
   * Gets the singleton ObjectMapper instance for JSON processing. Creates a new mapper if one
   * doesn't exist, configured with all necessary modules and settings for handling Trino-specific
   * types and plugin classes.
   *
   * @param appClassLoader the class loader to use for loading application classes
   * @return the configured ObjectMapper instance
   */
  public static ObjectMapper getMapper(ClassLoader appClassLoader) {
    if (mapper != null) {
      return mapper;
    }

    synchronized (JsonCodec.class) {
      if (mapper != null) {
        return mapper;
      }
      mapper = buildMapper(appClassLoader);
      return mapper;
    }
  }

  public static Type getJsonType(ClassLoader classLoader) {
    if (jsonType != null) {
      return jsonType;
    }

    synchronized (JsonCodec.class) {
      if (jsonType != null) {
        return jsonType;
      }
      jsonType = buildJsonType(classLoader);
      return jsonType;
    }
  }
}
