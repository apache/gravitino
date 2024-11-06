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
package org.apache.gravitino.authorization.ranger;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.authorization.Privilege;
import org.apache.gravitino.authorization.SecurableObject;
import org.apache.gravitino.authorization.SecurableObjects;
import org.apache.gravitino.authorization.ranger.RangerPrivileges.RangerHivePrivilege;
import org.apache.gravitino.authorization.ranger.reference.RangerDefines.PolicyResource;
import org.apache.gravitino.exceptions.AuthorizationPluginException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RangerAuthorizationHadoopSQLPlugin extends RangerAuthorizationPlugin {
  private static final Logger LOG =
      LoggerFactory.getLogger(RangerAuthorizationHadoopSQLPlugin.class);
  private static volatile RangerAuthorizationHadoopSQLPlugin instance = null;

  private RangerAuthorizationHadoopSQLPlugin(Map<String, String> config) {
    super(config);
  }

  public static synchronized RangerAuthorizationHadoopSQLPlugin getInstance(
      Map<String, String> config) {
    if (instance == null) {
      synchronized (RangerAuthorizationHadoopSQLPlugin.class) {
        if (instance == null) {
          instance = new RangerAuthorizationHadoopSQLPlugin(config);
        }
      }
    }
    return instance;
  }

  /** Validate different Ranger metadata object */
  @Override
  public void validateRangerMetadataObject(List<String> names, RangerMetadataObject.Type type)
      throws IllegalArgumentException {
    Preconditions.checkArgument(
        names != null && !names.isEmpty(), "Cannot create a Ranger metadata object with no names");
    Preconditions.checkArgument(
        names.size() <= 3,
        "Cannot create a Ranger metadata object with the name length which is greater than 3");
    Preconditions.checkArgument(
        type != null, "Cannot create a Ranger metadata object with no type");

    Preconditions.checkArgument(
        names.size() != 1 || type == RangerMetadataObject.Type.SCHEMA,
        "If the length of names is 1, it must be the SCHEMA type");

    Preconditions.checkArgument(
        names.size() != 2 || type == RangerMetadataObject.Type.TABLE,
        "If the length of names is 2, it must be the TABLE type");

    Preconditions.checkArgument(
        names.size() != 3 || type == RangerMetadataObject.Type.COLUMN,
        "If the length of names is 3, it must be COLUMN");

    for (String name : names) {
      RangerMetadataObjects.checkName(name);
    }
  }

  @Override
  /** Set the default mapping Gravitino privilege name to the Ranger rule */
  public Map<Privilege.Name, Set<RangerPrivilege>> privilegesMappingRule() {
    return ImmutableMap.of(
        Privilege.Name.CREATE_CATALOG,
        ImmutableSet.of(RangerHivePrivilege.CREATE),
        Privilege.Name.USE_CATALOG,
        ImmutableSet.of(RangerHivePrivilege.SELECT),
        Privilege.Name.CREATE_SCHEMA,
        ImmutableSet.of(RangerHivePrivilege.CREATE),
        Privilege.Name.USE_SCHEMA,
        ImmutableSet.of(RangerHivePrivilege.SELECT),
        Privilege.Name.CREATE_TABLE,
        ImmutableSet.of(RangerHivePrivilege.CREATE),
        Privilege.Name.MODIFY_TABLE,
        ImmutableSet.of(
            RangerHivePrivilege.UPDATE, RangerHivePrivilege.ALTER, RangerHivePrivilege.WRITE),
        Privilege.Name.SELECT_TABLE,
        ImmutableSet.of(RangerHivePrivilege.READ, RangerHivePrivilege.SELECT));
  }

  @Override
  /** Set the default owner rule. */
  public Set<RangerPrivilege> ownerMappingRule() {
    return ImmutableSet.of(RangerHivePrivilege.ALL);
  }

  @Override
  /** Set Ranger policy resource rule. */
  public List<String> policyResourceDefinesRule() {
    return ImmutableList.of(
        PolicyResource.DATABASE.getName(),
        PolicyResource.TABLE.getName(),
        PolicyResource.COLUMN.getName());
  }

  @Override
  /** Allow privilege operation defines rule. */
  public Set<Privilege.Name> allowPrivilegesRule() {
    return ImmutableSet.of(
        Privilege.Name.CREATE_CATALOG,
        Privilege.Name.USE_CATALOG,
        Privilege.Name.CREATE_SCHEMA,
        Privilege.Name.USE_SCHEMA,
        Privilege.Name.CREATE_TABLE,
        Privilege.Name.MODIFY_TABLE,
        Privilege.Name.SELECT_TABLE);
  }

  /**
   * Allow Gravitino MetadataObject type defines rule.
   *
   * @return The allow Gravitino MetadataObject type defines rule.
   */
  @Override
  public Set<MetadataObject.Type> allowMetadataObjectTypesRule() {
    return ImmutableSet.of(
        MetadataObject.Type.METALAKE,
        MetadataObject.Type.CATALOG,
        MetadataObject.Type.SCHEMA,
        MetadataObject.Type.TABLE,
        MetadataObject.Type.COLUMN);
  }

  /** Translate the Gravitino securable object to the Ranger owner securable object. */
  @Override
  public List<RangerSecurableObject> translateOwner(MetadataObject gravitinoMetadataObject) {
    List<RangerSecurableObject> rangerSecurableObjects = new ArrayList<>();

    switch (gravitinoMetadataObject.type()) {
      case METALAKE:
      case CATALOG:
        // Add `*` for the SCHEMA permission
        rangerSecurableObjects.add(
            generateRangerSecurableObject(
                ImmutableList.of(RangerHelper.RESOURCE_ALL),
                RangerMetadataObject.Type.SCHEMA,
                ownerMappingRule()));
        // Add `*.*` for the TABLE permission
        rangerSecurableObjects.add(
            generateRangerSecurableObject(
                ImmutableList.of(RangerHelper.RESOURCE_ALL, RangerHelper.RESOURCE_ALL),
                RangerMetadataObject.Type.TABLE,
                ownerMappingRule()));
        // Add `*.*.*` for the COLUMN permission
        rangerSecurableObjects.add(
            generateRangerSecurableObject(
                ImmutableList.of(
                    RangerHelper.RESOURCE_ALL,
                    RangerHelper.RESOURCE_ALL,
                    RangerHelper.RESOURCE_ALL),
                RangerMetadataObject.Type.COLUMN,
                ownerMappingRule()));
        break;
      case SCHEMA:
        // Add `{schema}` for the SCHEMA permission
        rangerSecurableObjects.add(
            generateRangerSecurableObject(
                ImmutableList.of(gravitinoMetadataObject.name() /*Schema name*/),
                RangerMetadataObject.Type.SCHEMA,
                ownerMappingRule()));
        // Add `{schema}.*` for the TABLE permission
        rangerSecurableObjects.add(
            generateRangerSecurableObject(
                ImmutableList.of(
                    gravitinoMetadataObject.name() /*Schema name*/, RangerHelper.RESOURCE_ALL),
                RangerMetadataObject.Type.TABLE,
                ownerMappingRule()));
        // Add `{schema}.*.*` for the COLUMN permission
        rangerSecurableObjects.add(
            generateRangerSecurableObject(
                ImmutableList.of(
                    gravitinoMetadataObject.name() /*Schema name*/,
                    RangerHelper.RESOURCE_ALL,
                    RangerHelper.RESOURCE_ALL),
                RangerMetadataObject.Type.COLUMN,
                ownerMappingRule()));
        break;
      case TABLE:
        // Add `{schema}.{table}` for the TABLE permission
        rangerSecurableObjects.add(
            generateRangerSecurableObject(
                translateMetadataObject(gravitinoMetadataObject).names(),
                RangerMetadataObject.Type.TABLE,
                ownerMappingRule()));
        // Add `{schema}.{table}.*` for the COLUMN permission
        rangerSecurableObjects.add(
            generateRangerSecurableObject(
                Stream.concat(
                        translateMetadataObject(gravitinoMetadataObject).names().stream(),
                        Stream.of(RangerHelper.RESOURCE_ALL))
                    .collect(Collectors.toList()),
                RangerMetadataObject.Type.COLUMN,
                ownerMappingRule()));
        break;
      default:
        throw new AuthorizationPluginException(
            "The owner privilege is not supported for the securable object: %s",
            gravitinoMetadataObject.type());
    }

    return rangerSecurableObjects;
  }

  /** Translate the Gravitino securable object to the Ranger securable object. */
  @Override
  public List<RangerSecurableObject> translatePrivilege(SecurableObject securableObject) {
    List<RangerSecurableObject> rangerSecurableObjects = new ArrayList<>();

    securableObject.privileges().stream()
        .filter(Objects::nonNull)
        .forEach(
            gravitinoPrivilege -> {
              Set<RangerPrivilege> rangerPrivileges = new HashSet<>();
              // Ignore unsupported privileges
              if (!privilegesMappingRule().containsKey(gravitinoPrivilege.name())) {
                return;
              }
              privilegesMappingRule().get(gravitinoPrivilege.name()).stream()
                  .forEach(
                      rangerPrivilege ->
                          rangerPrivileges.add(
                              new RangerPrivileges.RangerHivePrivilegeImpl(
                                  rangerPrivilege, gravitinoPrivilege.condition())));

              switch (gravitinoPrivilege.name()) {
                case CREATE_CATALOG:
                  // Ignore the Gravitino privilege `CREATE_CATALOG` in the
                  // RangerAuthorizationHivePlugin
                  break;
                case USE_CATALOG:
                  switch (securableObject.type()) {
                    case METALAKE:
                    case CATALOG:
                      // Add Ranger privilege(`SELECT`) to SCHEMA(`*`)
                      rangerSecurableObjects.add(
                          generateRangerSecurableObject(
                              ImmutableList.of(RangerHelper.RESOURCE_ALL),
                              RangerMetadataObject.Type.SCHEMA,
                              rangerPrivileges));
                      break;
                    default:
                      throw new AuthorizationPluginException(
                          "The privilege %s is not supported for the securable object: %s",
                          gravitinoPrivilege.name(), securableObject.type());
                  }
                  break;
                case CREATE_SCHEMA:
                  switch (securableObject.type()) {
                    case METALAKE:
                    case CATALOG:
                      // Add Ranger privilege(`CREATE`) to SCHEMA(`*`)
                      rangerSecurableObjects.add(
                          generateRangerSecurableObject(
                              ImmutableList.of(RangerHelper.RESOURCE_ALL),
                              RangerMetadataObject.Type.SCHEMA,
                              rangerPrivileges));
                      break;
                    default:
                      throw new AuthorizationPluginException(
                          "The privilege %s is not supported for the securable object: %s",
                          gravitinoPrivilege.name(), securableObject.type());
                  }
                  break;
                case USE_SCHEMA:
                  switch (securableObject.type()) {
                    case METALAKE:
                    case CATALOG:
                      // Add Ranger privilege(`SELECT`) to SCHEMA(`*`)
                      rangerSecurableObjects.add(
                          generateRangerSecurableObject(
                              ImmutableList.of(RangerHelper.RESOURCE_ALL),
                              RangerMetadataObject.Type.SCHEMA,
                              rangerPrivileges));
                      break;
                    case SCHEMA:
                      // Add Ranger privilege(`SELECT`) to SCHEMA(`{schema}`)
                      rangerSecurableObjects.add(
                          generateRangerSecurableObject(
                              ImmutableList.of(securableObject.name() /*Schema name*/),
                              RangerMetadataObject.Type.SCHEMA,
                              rangerPrivileges));
                      break;
                    default:
                      throw new AuthorizationPluginException(
                          "The privilege %s is not supported for the securable object: %s",
                          gravitinoPrivilege.name(), securableObject.type());
                  }
                  break;
                case CREATE_TABLE:
                case MODIFY_TABLE:
                case SELECT_TABLE:
                  switch (securableObject.type()) {
                    case METALAKE:
                    case CATALOG:
                      // Add `*.*` for the TABLE permission
                      rangerSecurableObjects.add(
                          generateRangerSecurableObject(
                              ImmutableList.of(
                                  RangerHelper.RESOURCE_ALL, RangerHelper.RESOURCE_ALL),
                              RangerMetadataObject.Type.TABLE,
                              rangerPrivileges));
                      // Add `*.*.*` for the COLUMN permission
                      rangerSecurableObjects.add(
                          generateRangerSecurableObject(
                              ImmutableList.of(
                                  RangerHelper.RESOURCE_ALL,
                                  RangerHelper.RESOURCE_ALL,
                                  RangerHelper.RESOURCE_ALL),
                              RangerMetadataObject.Type.COLUMN,
                              rangerPrivileges));
                      break;
                    case SCHEMA:
                      // Add `{schema}.*` for the TABLE permission
                      rangerSecurableObjects.add(
                          generateRangerSecurableObject(
                              ImmutableList.of(
                                  securableObject.name() /*Schema name*/,
                                  RangerHelper.RESOURCE_ALL),
                              RangerMetadataObject.Type.TABLE,
                              rangerPrivileges));
                      // Add `{schema}.*.*` for the COLUMN permission
                      rangerSecurableObjects.add(
                          generateRangerSecurableObject(
                              ImmutableList.of(
                                  securableObject.name() /*Schema name*/,
                                  RangerHelper.RESOURCE_ALL,
                                  RangerHelper.RESOURCE_ALL),
                              RangerMetadataObject.Type.COLUMN,
                              rangerPrivileges));
                      break;
                    case TABLE:
                      if (gravitinoPrivilege.name() == Privilege.Name.CREATE_TABLE) {
                        throw new AuthorizationPluginException(
                            "The privilege %s is not supported for the securable object: %s",
                            gravitinoPrivilege.name(), securableObject.type());
                      } else {
                        // Add `{schema}.{table}` for the TABLE permission
                        rangerSecurableObjects.add(
                            generateRangerSecurableObject(
                                translateMetadataObject(securableObject).names(),
                                RangerMetadataObject.Type.TABLE,
                                rangerPrivileges));
                        // Add `{schema}.{table}.*` for the COLUMN permission
                        rangerSecurableObjects.add(
                            generateRangerSecurableObject(
                                Stream.concat(
                                        translateMetadataObject(securableObject).names().stream(),
                                        Stream.of(RangerHelper.RESOURCE_ALL))
                                    .collect(Collectors.toList()),
                                RangerMetadataObject.Type.COLUMN,
                                rangerPrivileges));
                      }
                      break;
                    default:
                      LOG.warn(
                          "RangerAuthorizationHivePlugin -> privilege {} is not supported for the securable object: {}",
                          gravitinoPrivilege.name(),
                          securableObject.type());
                  }
                  break;
                default:
                  LOG.warn(
                      "RangerAuthorizationHivePlugin -> privilege {} is not supported for the securable object: {}",
                      gravitinoPrivilege.name(),
                      securableObject.type());
              }
            });

    return rangerSecurableObjects;
  }

  /**
   * Because the Ranger metadata object is different from the Gravitino metadata object, we need to
   * convert the Gravitino metadata object to the Ranger metadata object.
   */
  @Override
  public RangerMetadataObject translateMetadataObject(MetadataObject metadataObject) {
    Preconditions.checkArgument(
        allowMetadataObjectTypesRule().contains(metadataObject.type()),
        String.format(
            "The metadata object type %s is not supported in the RangerAuthorizationHivePlugin",
            metadataObject.type()));
    Preconditions.checkArgument(
        !(metadataObject instanceof RangerPrivileges),
        "The metadata object must be not a RangerPrivileges object.");
    List<String> nsMetadataObject =
        Lists.newArrayList(SecurableObjects.DOT_SPLITTER.splitToList(metadataObject.fullName()));
    Preconditions.checkArgument(
        nsMetadataObject.size() > 0, "The metadata object must have at least one name.");

    RangerMetadataObject.Type type;
    if (metadataObject.type() == MetadataObject.Type.METALAKE
        || metadataObject.type() == MetadataObject.Type.CATALOG) {
      nsMetadataObject.clear();
      nsMetadataObject.add(RangerHelper.RESOURCE_ALL);
      type = RangerMetadataObject.Type.SCHEMA;
    } else {
      nsMetadataObject.remove(0); // Remove the catalog name
      type = RangerMetadataObject.Type.fromMetadataType(metadataObject.type());
    }

    validateRangerMetadataObject(nsMetadataObject, type);
    return new RangerMetadataObjects.RangerMetadataObjectImpl(
        RangerMetadataObjects.getParentFullName(nsMetadataObject),
        RangerMetadataObjects.getLastName(nsMetadataObject),
        type);
  }
}
