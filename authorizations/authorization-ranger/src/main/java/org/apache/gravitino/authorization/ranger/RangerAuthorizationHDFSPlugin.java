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

import com.google.common.annotations.VisibleForTesting;
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
import java.util.regex.Pattern;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.Schema;
import org.apache.gravitino.authorization.AuthorizationMetadataObject;
import org.apache.gravitino.authorization.AuthorizationPrivilege;
import org.apache.gravitino.authorization.AuthorizationSecurableObject;
import org.apache.gravitino.authorization.Privilege;
import org.apache.gravitino.authorization.SecurableObject;
import org.apache.gravitino.authorization.SecurableObjects;
import org.apache.gravitino.authorization.common.PathBasedMetadataObject;
import org.apache.gravitino.authorization.common.PathBasedSecurableObject;
import org.apache.gravitino.authorization.ranger.reference.RangerDefines;
import org.apache.gravitino.catalog.FilesetDispatcher;
import org.apache.gravitino.catalog.hive.HiveConstants;
import org.apache.gravitino.exceptions.AuthorizationPluginException;
import org.apache.gravitino.exceptions.NoSuchEntityException;
import org.apache.gravitino.file.Fileset;
import org.apache.ranger.plugin.model.RangerPolicy;

public class RangerAuthorizationHDFSPlugin extends RangerAuthorizationPlugin {
  private static final Pattern pattern = Pattern.compile("^hdfs://[^/]*");

  public RangerAuthorizationHDFSPlugin(String metalake, Map<String, String> config) {
    super(metalake, config);
  }

  @Override
  public Map<Privilege.Name, Set<AuthorizationPrivilege>> privilegesMappingRule() {
    return ImmutableMap.of(
        Privilege.Name.USE_CATALOG,
        ImmutableSet.of(
            RangerPrivileges.RangerHdfsPrivilege.READ,
            RangerPrivileges.RangerHdfsPrivilege.EXECUTE),
        Privilege.Name.CREATE_CATALOG,
        ImmutableSet.of(
            RangerPrivileges.RangerHdfsPrivilege.READ,
            RangerPrivileges.RangerHdfsPrivilege.WRITE,
            RangerPrivileges.RangerHdfsPrivilege.EXECUTE),
        Privilege.Name.USE_SCHEMA,
        ImmutableSet.of(
            RangerPrivileges.RangerHdfsPrivilege.READ,
            RangerPrivileges.RangerHdfsPrivilege.EXECUTE),
        Privilege.Name.CREATE_SCHEMA,
        ImmutableSet.of(
            RangerPrivileges.RangerHdfsPrivilege.READ,
            RangerPrivileges.RangerHdfsPrivilege.WRITE,
            RangerPrivileges.RangerHdfsPrivilege.EXECUTE),
        Privilege.Name.CREATE_TABLE,
        ImmutableSet.of(
            RangerPrivileges.RangerHdfsPrivilege.READ,
            RangerPrivileges.RangerHdfsPrivilege.WRITE,
            RangerPrivileges.RangerHdfsPrivilege.EXECUTE),
        Privilege.Name.MODIFY_TABLE,
        ImmutableSet.of(
            RangerPrivileges.RangerHdfsPrivilege.READ,
            RangerPrivileges.RangerHdfsPrivilege.WRITE,
            RangerPrivileges.RangerHdfsPrivilege.EXECUTE),
        Privilege.Name.SELECT_TABLE,
        ImmutableSet.of(
            RangerPrivileges.RangerHdfsPrivilege.READ,
            RangerPrivileges.RangerHdfsPrivilege.EXECUTE),
        Privilege.Name.READ_FILESET,
        ImmutableSet.of(
            RangerPrivileges.RangerHdfsPrivilege.READ,
            RangerPrivileges.RangerHdfsPrivilege.EXECUTE),
        Privilege.Name.WRITE_FILESET,
        ImmutableSet.of(
            RangerPrivileges.RangerHdfsPrivilege.WRITE,
            RangerPrivileges.RangerHdfsPrivilege.EXECUTE));
  }

  @Override
  public Set<AuthorizationPrivilege> ownerMappingRule() {
    return ImmutableSet.of(
        RangerPrivileges.RangerHdfsPrivilege.READ,
        RangerPrivileges.RangerHdfsPrivilege.WRITE,
        RangerPrivileges.RangerHdfsPrivilege.EXECUTE);
  }

  @Override
  public List<String> policyResourceDefinesRule() {
    return ImmutableList.of(RangerDefines.PolicyResource.PATH.getName());
  }

  @Override
  protected RangerPolicy createPolicyAddResources(AuthorizationMetadataObject metadataObject) {
    RangerPolicy policy = new RangerPolicy();
    policy.setService(rangerServiceName);
    policy.setName(metadataObject.fullName());
    RangerPolicy.RangerPolicyResource policyResource =
        new RangerPolicy.RangerPolicyResource(metadataObject.names().get(0), false, true);
    policy.getResources().put(RangerDefines.PolicyResource.PATH.getName(), policyResource);
    return policy;
  }

  @Override
  public AuthorizationSecurableObject generateAuthorizationSecurableObject(
      List<String> names,
      AuthorizationMetadataObject.Type type,
      Set<AuthorizationPrivilege> privileges) {
    AuthorizationMetadataObject authMetadataObject =
        new PathBasedMetadataObject(AuthorizationMetadataObject.getLastName(names), type);
    authMetadataObject.validateAuthorizationMetadataObject();
    return new PathBasedSecurableObject(
        authMetadataObject.name(), authMetadataObject.type(), privileges);
  }

  @Override
  public Set<Privilege.Name> allowPrivilegesRule() {
    return ImmutableSet.of(
        Privilege.Name.CREATE_FILESET, Privilege.Name.READ_FILESET, Privilege.Name.WRITE_FILESET);
  }

  @Override
  public Set<MetadataObject.Type> allowMetadataObjectTypesRule() {
    return ImmutableSet.of(
        MetadataObject.Type.FILESET,
        MetadataObject.Type.SCHEMA,
        MetadataObject.Type.CATALOG,
        MetadataObject.Type.METALAKE);
  }

  @Override
  public List<AuthorizationSecurableObject> translatePrivilege(SecurableObject securableObject) {
    List<AuthorizationSecurableObject> rangerSecurableObjects = new ArrayList<>();

    securableObject.privileges().stream()
        .filter(Objects::nonNull)
        .forEach(
            gravitinoPrivilege -> {
              Set<AuthorizationPrivilege> rangerPrivileges = new HashSet<>();
              // Ignore unsupported privileges
              if (!privilegesMappingRule().containsKey(gravitinoPrivilege.name())) {
                return;
              }
              privilegesMappingRule().get(gravitinoPrivilege.name()).stream()
                  .forEach(
                      rangerPrivilege ->
                          rangerPrivileges.add(
                              new RangerPrivileges.RangerHDFSPrivilegeImpl(
                                  rangerPrivilege, gravitinoPrivilege.condition())));
              switch (gravitinoPrivilege.name()) {
                case USE_CATALOG:
                case CREATE_CATALOG:
                  // When HDFS is used as the Hive storage layer, Hive does not support the
                  // `USE_CATALOG` and `CREATE_CATALOG` privileges. So, we ignore these
                  // in the RangerAuthorizationHDFSPlugin.
                  break;
                case USE_SCHEMA:
                  break;
                case CREATE_SCHEMA:
                  switch (securableObject.type()) {
                    case METALAKE:
                    case CATALOG:
                      {
                        String locationPath = getLocationPath(securableObject);
                        if (locationPath != null && !locationPath.isEmpty()) {
                          PathBasedMetadataObject rangerPathBaseMetadataObject =
                              new PathBasedMetadataObject(
                                  locationPath, PathBasedMetadataObject.Type.PATH);
                          rangerSecurableObjects.add(
                              generateAuthorizationSecurableObject(
                                  rangerPathBaseMetadataObject.names(),
                                  PathBasedMetadataObject.Type.PATH,
                                  rangerPrivileges));
                        }
                      }
                      break;
                    case FILESET:
                      rangerSecurableObjects.add(
                          generateAuthorizationSecurableObject(
                              translateMetadataObject(securableObject).names(),
                              PathBasedMetadataObject.Type.PATH,
                              rangerPrivileges));
                      break;
                    default:
                      throw new AuthorizationPluginException(
                          "The privilege %s is not supported for the securable object: %s",
                          gravitinoPrivilege.name(), securableObject.type());
                  }
                  break;
                case SELECT_TABLE:
                case CREATE_TABLE:
                case MODIFY_TABLE:
                  break;
                case CREATE_FILESET:
                  // Ignore the Gravitino privilege `CREATE_FILESET` in the
                  // RangerAuthorizationHDFSPlugin
                  break;
                case READ_FILESET:
                case WRITE_FILESET:
                  switch (securableObject.type()) {
                    case METALAKE:
                    case CATALOG:
                    case SCHEMA:
                      break;
                    case FILESET:
                      rangerSecurableObjects.add(
                          generateAuthorizationSecurableObject(
                              translateMetadataObject(securableObject).names(),
                              PathBasedMetadataObject.Type.PATH,
                              rangerPrivileges));
                      break;
                    default:
                      throw new AuthorizationPluginException(
                          "The privilege %s is not supported for the securable object: %s",
                          gravitinoPrivilege.name(), securableObject.type());
                  }
                  break;
                default:
                  throw new AuthorizationPluginException(
                      "The privilege %s is not supported for the securable object: %s",
                      gravitinoPrivilege.name(), securableObject.type());
              }
            });

    return rangerSecurableObjects;
  }

  @Override
  public List<AuthorizationSecurableObject> translateOwner(MetadataObject gravitinoMetadataObject) {
    List<AuthorizationSecurableObject> rangerSecurableObjects = new ArrayList<>();
    switch (gravitinoMetadataObject.type()) {
      case METALAKE:
      case CATALOG:
      case SCHEMA:
        break;
      case FILESET:
        rangerSecurableObjects.add(
            generateAuthorizationSecurableObject(
                translateMetadataObject(gravitinoMetadataObject).names(),
                PathBasedMetadataObject.Type.PATH,
                ownerMappingRule()));
        break;
      default:
        throw new AuthorizationPluginException(
            "The owner privilege is not supported for the securable object: %s",
            gravitinoMetadataObject.type());
    }

    return rangerSecurableObjects;
  }

  @Override
  public AuthorizationMetadataObject translateMetadataObject(MetadataObject metadataObject) {
    Preconditions.checkArgument(
        allowMetadataObjectTypesRule().contains(metadataObject.type()),
        String.format(
            "The metadata object type %s is not supported in the RangerAuthorizationHDFSPlugin",
            metadataObject.type()));
    List<String> nsMetadataObject =
        Lists.newArrayList(SecurableObjects.DOT_SPLITTER.splitToList(metadataObject.fullName()));
    Preconditions.checkArgument(
        nsMetadataObject.size() > 0, "The metadata object must have at least one name.");

    PathBasedMetadataObject rangerPathBaseMetadataObject;
    switch (metadataObject.type()) {
      case METALAKE:
      case CATALOG:
        rangerPathBaseMetadataObject =
            new PathBasedMetadataObject("", PathBasedMetadataObject.Type.PATH);
        break;
      case SCHEMA:
        rangerPathBaseMetadataObject =
            new PathBasedMetadataObject(
                metadataObject.fullName(), PathBasedMetadataObject.Type.PATH);
        break;
      case FILESET:
        rangerPathBaseMetadataObject =
            new PathBasedMetadataObject(
                getLocationPath(metadataObject), PathBasedMetadataObject.Type.PATH);
        break;
      default:
        throw new AuthorizationPluginException(
            "The metadata object type %s is not supported in the RangerAuthorizationHDFSPlugin",
            metadataObject.type());
    }
    rangerPathBaseMetadataObject.validateAuthorizationMetadataObject();
    return rangerPathBaseMetadataObject;
  }

  private NameIdentifier getObjectNameIdentifier(MetadataObject metadataObject) {
    return NameIdentifier.parse(String.format("%s.%s", metalake, metadataObject.fullName()));
  }

  @VisibleForTesting
  public String getLocationPath(MetadataObject metadataObject) throws NoSuchEntityException {
    String locationPath = null;
    switch (metadataObject.type()) {
      case METALAKE:
      case SCHEMA:
      case TABLE:
        break;
      case CATALOG:
        {
          Namespace nsMetadataObj = Namespace.fromString(metadataObject.fullName());
          NameIdentifier ident = NameIdentifier.of(metalake, nsMetadataObj.level(0));
          Catalog catalog = GravitinoEnv.getInstance().catalogDispatcher().loadCatalog(ident);
          if (catalog.provider().equals("hive")) {
            Schema schema =
                GravitinoEnv.getInstance()
                    .schemaDispatcher()
                    .loadSchema(
                        NameIdentifier.of(
                            metalake, nsMetadataObj.level(0), "default" /*Hive default schema*/));
            String defaultSchemaLocation = schema.properties().get(HiveConstants.LOCATION);
            locationPath = pattern.matcher(defaultSchemaLocation).replaceAll("");
          }
        }
        break;
      case FILESET:
        FilesetDispatcher filesetDispatcher = GravitinoEnv.getInstance().filesetDispatcher();
        NameIdentifier identifier = getObjectNameIdentifier(metadataObject);
        Fileset fileset = filesetDispatcher.loadFileset(identifier);
        Preconditions.checkArgument(
            fileset != null, String.format("Fileset %s is not found", identifier));
        String filesetLocation = fileset.storageLocation();
        Preconditions.checkArgument(
            filesetLocation != null, String.format("Fileset %s location is not found", identifier));
        locationPath = pattern.matcher(filesetLocation).replaceAll("");
        break;
      default:
        throw new AuthorizationPluginException(
            "The metadata object type %s is not supported in the RangerAuthorizationHDFSPlugin",
            metadataObject.type());
    }
    return locationPath;
  }
}
