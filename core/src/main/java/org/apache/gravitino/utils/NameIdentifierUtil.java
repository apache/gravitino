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
package org.apache.gravitino.utils;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.gravitino.Entity;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.MetadataObjects;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.authorization.AuthorizationUtils;
import org.apache.gravitino.exceptions.IllegalNameIdentifierException;

/**
 * A name identifier is a sequence of names separated by dots. It's used to identify a metalake, a
 * catalog, a schema or a table. For example, "metalake1" can represent a metalake,
 * "metalake1.catalog1" can represent a catalog, "metalake1.catalog1.schema1" can represent a
 * schema.
 */
public class NameIdentifierUtil {

  private NameIdentifierUtil() {}

  /**
   * Create the metalake {@link NameIdentifierUtil} with the given name.
   *
   * @param metalake The metalake name
   * @return The created metalake {@link NameIdentifier}
   */
  public static NameIdentifier ofMetalake(String metalake) {
    return NameIdentifier.of(metalake);
  }

  /**
   * Create the catalog {@link NameIdentifierUtil} with the given metalake and catalog name.
   *
   * @param metalake The metalake name
   * @param catalog The catalog name
   * @return The created catalog {@link NameIdentifier}
   */
  public static NameIdentifier ofCatalog(String metalake, String catalog) {
    return NameIdentifier.of(metalake, catalog);
  }

  /**
   * Create the schema {@link NameIdentifierUtil} with the given metalake, catalog and schema name.
   *
   * @param metalake The metalake name
   * @param catalog The catalog name
   * @param schema The schema name
   * @return The created schema {@link NameIdentifierUtil}
   */
  public static NameIdentifier ofSchema(String metalake, String catalog, String schema) {
    return NameIdentifier.of(metalake, catalog, schema);
  }

  /**
   * Create the table {@link NameIdentifier} with the given metalake, catalog, schema and table
   * name.
   *
   * @param metalake The metalake name
   * @param catalog The catalog name
   * @param schema The schema name
   * @param table The table name
   * @return The created table {@link NameIdentifier}
   */
  public static NameIdentifier ofTable(
      String metalake, String catalog, String schema, String table) {
    return NameIdentifier.of(metalake, catalog, schema, table);
  }

  /**
   * Create the tag {@link NameIdentifier} with the given metalake and tag name.
   *
   * @param metalake The metalake name
   * @param tagName The tag name
   * @return The created tag {@link NameIdentifier}
   */
  public static NameIdentifier ofTag(String metalake, String tagName) {
    return NameIdentifier.of(NamespaceUtil.ofTag(metalake), tagName);
  }

  /**
   * Create the policy {@link NameIdentifier} with the given metalake and policy name.
   *
   * @param metalake The metalake name
   * @param policyName The policy name
   * @return the created policy {@link NameIdentifier}
   */
  public static NameIdentifier ofPolicy(String metalake, String policyName) {
    return NameIdentifier.of(NamespaceUtil.ofPolicy(metalake), policyName);
  }

  /**
   * Create the user {@link NameIdentifier} with the given metalake and username.
   *
   * @param metalake The metalake name
   * @param userName The username
   * @return the created user {@link NameIdentifier}
   */
  public static NameIdentifier ofUser(String metalake, String userName) {
    return NameIdentifier.of(NamespaceUtil.ofUser(metalake), userName);
  }

  /**
   * Create the role {@link NameIdentifier} with the given metalake and role name.
   *
   * @param metalake The metalake name
   * @param roleName The role name
   * @return the created role {@link NameIdentifier}
   */
  public static NameIdentifier ofRole(String metalake, String roleName) {
    return NameIdentifier.of(NamespaceUtil.ofRole(metalake), roleName);
  }

  /**
   * Create the column {@link NameIdentifier} with the given metalake, catalog, schema, table and
   * column name.
   *
   * @param metalake The metalake name
   * @param catalog The catalog name
   * @param schema The schema name
   * @param table The table name
   * @param column The column name
   * @return The created column {@link NameIdentifier}
   */
  public static NameIdentifier ofColumn(
      String metalake, String catalog, String schema, String table, String column) {
    return NameIdentifier.of(metalake, catalog, schema, table, column);
  }

  /**
   * Create the fileset {@link NameIdentifier} with the given metalake, catalog, schema and fileset
   * name.
   *
   * @param metalake The metalake name
   * @param catalog The catalog name
   * @param schema The schema name
   * @param fileset The fileset name
   * @return The created fileset {@link NameIdentifier}
   */
  public static NameIdentifier ofFileset(
      String metalake, String catalog, String schema, String fileset) {
    return NameIdentifier.of(metalake, catalog, schema, fileset);
  }

  /**
   * Create the topic {@link NameIdentifier} with the given metalake, catalog, schema and topic
   * name.
   *
   * @param metalake The metalake name
   * @param catalog The catalog name
   * @param schema The schema name
   * @param topic The topic name
   * @return The created topic {@link NameIdentifier}
   */
  public static NameIdentifier ofTopic(
      String metalake, String catalog, String schema, String topic) {
    return NameIdentifier.of(metalake, catalog, schema, topic);
  }

  /**
   * Create the model {@link NameIdentifier} with the given metalake, catalog, schema and model
   * name.
   *
   * @param metalake The metalake name
   * @param catalog The catalog name
   * @param schema The schema name
   * @param model The model name
   * @return The created model {@link NameIdentifier}
   */
  public static NameIdentifier ofModel(
      String metalake, String catalog, String schema, String model) {
    return NameIdentifier.of(metalake, catalog, schema, model);
  }

  /**
   * Create the model {@link NameIdentifier} from the give model version's namespace.
   *
   * @param modelVersionNs The model version's namespace
   * @return The created model {@link NameIdentifier}
   */
  public static NameIdentifier toModelIdentifier(Namespace modelVersionNs) {
    return NameIdentifier.of(modelVersionNs.levels());
  }

  /**
   * Create the model {@link NameIdentifier} from the give model version's name identifier.
   *
   * @param modelIdent The model version's name identifier
   * @return The created model {@link NameIdentifier}
   */
  public static NameIdentifier toModelIdentifier(NameIdentifier modelIdent) {
    return NameIdentifier.of(modelIdent.namespace().levels());
  }

  /**
   * Create the model version {@link NameIdentifier} with the given metalake, catalog, schema, model
   * and version.
   *
   * @param metalake The metalake name
   * @param catalog The catalog name
   * @param schema The schema name
   * @param model The model name
   * @param version The model version
   * @return The created model version {@link NameIdentifier}
   */
  public static NameIdentifier ofModelVersion(
      String metalake, String catalog, String schema, String model, int version) {
    return NameIdentifier.of(metalake, catalog, schema, model, String.valueOf(version));
  }

  /**
   * Create the model version {@link NameIdentifier} with the given metalake, catalog, schema, model
   * and alias.
   *
   * @param metalake The metalake name
   * @param catalog The catalog name
   * @param schema The schema name
   * @param model The model name
   * @param alias The model version alias
   * @return The created model version {@link NameIdentifier}
   */
  public static NameIdentifier ofModelVersion(
      String metalake, String catalog, String schema, String model, String alias) {
    return NameIdentifier.of(metalake, catalog, schema, model, alias);
  }

  /**
   * Create the model version {@link NameIdentifier} with the given model identifier and version.
   *
   * @param modelIdent The model identifier
   * @param version The model version
   * @return The created model version {@link NameIdentifier}
   */
  public static NameIdentifier toModelVersionIdentifier(NameIdentifier modelIdent, int version) {
    return ofModelVersion(
        modelIdent.namespace().level(0),
        modelIdent.namespace().level(1),
        modelIdent.namespace().level(2),
        modelIdent.name(),
        version);
  }

  /**
   * Create the model version {@link NameIdentifier} with the given model identifier and alias.
   *
   * @param modelIdent The model identifier
   * @param alias The model version alias
   * @return The created model version {@link NameIdentifier}
   */
  public static NameIdentifier toModelVersionIdentifier(NameIdentifier modelIdent, String alias) {
    return ofModelVersion(
        modelIdent.namespace().level(0),
        modelIdent.namespace().level(1),
        modelIdent.namespace().level(2),
        modelIdent.name(),
        alias);
  }

  /**
   * Create the job template {@link NameIdentifier} with the given metalake and job template name.
   *
   * @param metalake The metalake name
   * @param jobTemplateName The job template name
   * @return The created job template {@link NameIdentifier}
   */
  public static NameIdentifier ofJobTemplate(String metalake, String jobTemplateName) {
    return NameIdentifier.of(NamespaceUtil.ofJobTemplate(metalake), jobTemplateName);
  }

  /**
   * Create the job {@link NameIdentifier} with the given metalake and job name.
   *
   * @param metalake The metalake name
   * @param jobName The job name
   * @return The created job {@link NameIdentifier}
   */
  public static NameIdentifier ofJob(String metalake, String jobName) {
    return NameIdentifier.of(NamespaceUtil.ofJob(metalake), jobName);
  }

  /**
   * Try to get the catalog {@link NameIdentifier} from the given {@link NameIdentifier}.
   *
   * @param ident The {@link NameIdentifier} to check.
   * @return The catalog {@link NameIdentifier}
   * @throws IllegalNameIdentifierException If the given {@link NameIdentifier} does not include
   *     catalog name
   */
  public static NameIdentifier getCatalogIdentifier(NameIdentifier ident)
      throws IllegalNameIdentifierException {
    NameIdentifier.check(
        ident.name() != null && !ident.name().isEmpty(),
        "The name variable in the NameIdentifier must have value.");
    Namespace.check(
        ident.namespace() != null && !ident.namespace().isEmpty(),
        "Catalog namespace must be non-null and have 1 level, the input namespace is %s",
        ident.namespace());

    List<String> allElems =
        Stream.concat(Arrays.stream(ident.namespace().levels()), Stream.of(ident.name()))
            .collect(Collectors.toList());
    if (allElems.size() < 2) {
      throw new IllegalNameIdentifierException(
          "Cannot create a catalog NameIdentifier less than two elements.");
    }
    return NameIdentifier.of(allElems.get(0), allElems.get(1));
  }

  /**
   * Try to get the schema {@link NameIdentifier} from the given {@link NameIdentifier}.
   *
   * @param ident The {@link NameIdentifier} to check.
   * @return The schema {@link NameIdentifier}
   * @throws IllegalNameIdentifierException If the given {@link NameIdentifier} does not include
   *     schema name
   */
  public static NameIdentifier getSchemaIdentifier(NameIdentifier ident)
      throws IllegalNameIdentifierException {
    NameIdentifier.check(
        ident.name() != null && !ident.name().isEmpty(),
        "The name variable in the NameIdentifier must have value.");
    Namespace.check(
        ident.namespace() != null && !ident.namespace().isEmpty() && ident.namespace().length() > 1,
        "Schema namespace must be non-null and at least 1 level, the input namespace is %s",
        ident.namespace());

    List<String> allElems =
        Stream.concat(Arrays.stream(ident.namespace().levels()), Stream.of(ident.name()))
            .collect(Collectors.toList());
    if (allElems.size() < 3) {
      throw new IllegalNameIdentifierException(
          "Cannot create a schema NameIdentifier less than three elements.");
    }
    return NameIdentifier.of(allElems.get(0), allElems.get(1), allElems.get(2));
  }

  /**
   * Check the given {@link NameIdentifier} is a metalake identifier. Throw an {@link
   * IllegalNameIdentifierException} if it's not.
   *
   * @param ident The metalake {@link NameIdentifier} to check.
   */
  public static void checkMetalake(NameIdentifier ident) {
    // We don't have to check the name field of NameIdentifier, it's already checked when
    // creating NameIdentifier object.
    NameIdentifier.check(ident != null, "Metalake identifier must not be null");
    NamespaceUtil.checkMetalake(ident.namespace());
  }

  /**
   * Check the given {@link NameIdentifier} is a catalog identifier. Throw an {@link
   * IllegalNameIdentifierException} if it's not.
   *
   * @param ident The catalog {@link NameIdentifier} to check.
   */
  public static void checkCatalog(NameIdentifier ident) {
    NameIdentifier.check(ident != null, "Catalog identifier must not be null");
    NamespaceUtil.checkCatalog(ident.namespace());
  }

  /**
   * Check the given {@link NameIdentifier} is a schema identifier. Throw an {@link
   * IllegalNameIdentifierException} if it's not.
   *
   * @param ident The schema {@link NameIdentifier} to check.
   */
  public static void checkSchema(NameIdentifier ident) {
    NameIdentifier.check(ident != null, "Schema identifier must not be null");
    NamespaceUtil.checkSchema(ident.namespace());
  }

  /**
   * Check the given {@link NameIdentifier} is a table identifier. Throw an {@link
   * IllegalNameIdentifierException} if it's not.
   *
   * @param ident The table {@link NameIdentifier} to check.
   */
  public static void checkTable(NameIdentifier ident) {
    NameIdentifier.check(ident != null, "Table identifier must not be null");
    NamespaceUtil.checkTable(ident.namespace());
  }

  /**
   * Check the given {@link NameIdentifier} is a column identifier. Throw an {@link
   * IllegalNameIdentifierException} if it's not.
   *
   * @param ident The column {@link NameIdentifier} to check.
   */
  public static void checkColumn(NameIdentifier ident) {
    NameIdentifier.check(ident != null, "Column identifier must not be null");
    NamespaceUtil.checkColumn(ident.namespace());
  }

  /**
   * Check the given {@link NameIdentifier} is a fileset identifier. Throw an {@link
   * IllegalNameIdentifierException} if it's not.
   *
   * @param ident The fileset {@link NameIdentifier} to check.
   */
  public static void checkFileset(NameIdentifier ident) {
    NameIdentifier.check(ident != null, "Fileset identifier must not be null");
    NamespaceUtil.checkFileset(ident.namespace());
  }

  /**
   * Check the given {@link NameIdentifier} is a topic identifier. Throw an {@link
   * IllegalNameIdentifierException} if it's not.
   *
   * @param ident The topic {@link NameIdentifier} to check.
   */
  public static void checkTopic(NameIdentifier ident) {
    NameIdentifier.check(ident != null, "Topic identifier must not be null");
    NamespaceUtil.checkTopic(ident.namespace());
  }

  /**
   * Check the given {@link NameIdentifier} is a model identifier. Throw an {@link
   * IllegalNameIdentifierException} if it's not.
   *
   * @param ident The model {@link NameIdentifier} to check.
   */
  public static void checkModel(NameIdentifier ident) {
    NameIdentifier.check(ident != null, "Model identifier must not be null");
    NamespaceUtil.checkModel(ident.namespace());
  }

  /**
   * Check the given {@link NameIdentifier} is a model version identifier. Throw an {@link
   * IllegalNameIdentifierException} if it's not.
   *
   * @param ident The model version {@link NameIdentifier} to check.
   */
  public static void checkModelVersion(NameIdentifier ident) {
    NameIdentifier.check(ident != null, "Model version identifier must not be null");
    NamespaceUtil.checkModelVersion(ident.namespace());
  }

  /**
   * Convert the given {@link NameIdentifier} and {@link Entity.EntityType} to {@link
   * MetadataObject}.
   *
   * @param ident The identifier
   * @param entityType The entity type
   * @return The converted {@link MetadataObject}
   */
  public static MetadataObject toMetadataObject(
      NameIdentifier ident, Entity.EntityType entityType) {
    Preconditions.checkArgument(
        ident != null && entityType != null, "The identifier and entity type must not be null");

    Joiner dot = Joiner.on(".");

    switch (entityType) {
      case METALAKE:
        checkMetalake(ident);
        return MetadataObjects.of(null, ident.name(), MetadataObject.Type.METALAKE);

      case CATALOG:
        checkCatalog(ident);
        return MetadataObjects.of(null, ident.name(), MetadataObject.Type.CATALOG);

      case SCHEMA:
        checkSchema(ident);
        String schemaParent = ident.namespace().level(1);
        return MetadataObjects.of(schemaParent, ident.name(), MetadataObject.Type.SCHEMA);

      case TABLE:
        checkTable(ident);
        String tableParent = dot.join(ident.namespace().level(1), ident.namespace().level(2));
        return MetadataObjects.of(tableParent, ident.name(), MetadataObject.Type.TABLE);

      case COLUMN:
        checkColumn(ident);
        Namespace columnNs = ident.namespace();
        String columnParent = dot.join(columnNs.level(1), columnNs.level(2), columnNs.level(3));
        return MetadataObjects.of(columnParent, ident.name(), MetadataObject.Type.COLUMN);

      case FILESET:
        checkFileset(ident);
        String filesetParent = dot.join(ident.namespace().level(1), ident.namespace().level(2));
        return MetadataObjects.of(filesetParent, ident.name(), MetadataObject.Type.FILESET);

      case TOPIC:
        checkTopic(ident);
        String topicParent = dot.join(ident.namespace().level(1), ident.namespace().level(2));
        return MetadataObjects.of(topicParent, ident.name(), MetadataObject.Type.TOPIC);

      case MODEL:
        checkModel(ident);
        String modelParent = dot.join(ident.namespace().level(1), ident.namespace().level(2));
        return MetadataObjects.of(modelParent, ident.name(), MetadataObject.Type.MODEL);

      case ROLE:
        AuthorizationUtils.checkRole(ident);
        return MetadataObjects.of(null, ident.name(), MetadataObject.Type.ROLE);

      default:
        throw new IllegalArgumentException(
            "Entity type " + entityType + " is not supported to convert to MetadataObject");
    }
  }

  /**
   * Get the metalake name of the given {@link NameIdentifier}.
   *
   * @param identifier The name identifier of the entity
   * @return metalake name
   */
  public static String getMetalake(NameIdentifier identifier) {
    if (identifier.hasNamespace()) {
      return identifier.namespace().level(0);
    } else {
      return identifier.name();
    }
  }

  /**
   * Create the group {@link NameIdentifier} with the given metalake and group name.
   *
   * @param metalake The metalake name
   * @param groupName The group name
   * @return the created group {@link NameIdentifier}
   */
  public static NameIdentifier ofGroup(String metalake, String groupName) {
    return NameIdentifier.of(NamespaceUtil.ofGroup(metalake), groupName);
  }

  /**
   * Create a statistic {@link NameIdentifier} from the given identifier and name. The statistic
   * belongs to the given identifier. For example, if the identifier is a table identifier, the
   * statistic will be created for that table.
   *
   * @param entityIdent The identifier to use.
   * @param name The name of the statistic
   * @return The created statistic of {@link NameIdentifier}
   */
  public static NameIdentifier ofStatistic(NameIdentifier entityIdent, String name) {
    return NameIdentifier.of(Namespace.fromString(entityIdent.toString()), name);
  }

  /**
   * Try to get the model {@link NameIdentifier} from the given {@link NameIdentifier}.
   *
   * @param ident The {@link NameIdentifier} to check.
   * @return The model {@link NameIdentifier}
   * @throws IllegalNameIdentifierException If the given {@link NameIdentifier} does not include
   *     model name
   */
  public static NameIdentifier getModelIdentifier(NameIdentifier ident) {
    NameIdentifier.check(
        ident.name() != null && !ident.name().isEmpty(),
        "The name variable in the NameIdentifier must have value.");
    Namespace.check(
        ident.namespace() != null
            && !ident.namespace().isEmpty()
            && ident.namespace().length() >= 4,
        "ModelVersion namespace must be non-null and at least 4 level, the input namespace is %s",
        ident.namespace());

    List<String> allElems =
        Stream.concat(Arrays.stream(ident.namespace().levels()), Stream.of(ident.name()))
            .collect(Collectors.toList());
    if (allElems.size() < 4) {
      throw new IllegalNameIdentifierException(
          "Cannot create a model NameIdentifier less than four elements.");
    }
    return NameIdentifier.of(allElems.get(0), allElems.get(1), allElems.get(2), allElems.get(3));
  }
}
