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
import java.util.regex.Pattern;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.authorization.AuthorizationMetadataObject;
import org.apache.gravitino.authorization.AuthorizationPrivilege;
import org.apache.gravitino.authorization.AuthorizationSecurableObject;
import org.apache.gravitino.authorization.Privilege;
import org.apache.gravitino.authorization.SecurableObject;
import org.apache.gravitino.authorization.SecurableObjects;
import org.apache.gravitino.authorization.ranger.reference.RangerDefines;
import org.apache.gravitino.exceptions.AuthorizationPluginException;
import org.apache.gravitino.file.Fileset;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RangerAuthorizationHDFSPlugin extends RangerAuthorizationPlugin {
  private static final Logger LOG = LoggerFactory.getLogger(RangerAuthorizationHDFSPlugin.class);

  private static final Pattern pattern = Pattern.compile("^hdfs://[^/]*");

  private RangerAuthorizationHDFSPlugin(Map<String, String> config) {
    super(config);
  }

  public static synchronized RangerAuthorizationHDFSPlugin getInstance(Map<String, String> config) {
    return new RangerAuthorizationHDFSPlugin(config);
  }

  @Override
  public Map<Privilege.Name, Set<AuthorizationPrivilege>> privilegesMappingRule() {
    return ImmutableMap.of(
        Privilege.Name.READ_FILESET,
        ImmutableSet.of(RangerPrivileges.RangerHdfsPrivilege.READ),
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
  public Set<Privilege.Name> allowPrivilegesRule() {
    return ImmutableSet.of(
        Privilege.Name.CREATE_FILESET, Privilege.Name.READ_FILESET, Privilege.Name.WRITE_FILESET);
  }

  @Override
  public Set<MetadataObject.Type> allowMetadataObjectTypesRule() {
    return ImmutableSet.of(MetadataObject.Type.FILESET);
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
                              new RangerPrivileges.RangerHivePrivilegeImpl(
                                  rangerPrivilege, gravitinoPrivilege.condition())));

              switch (gravitinoPrivilege.name()) {
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
                              ImmutableList.of(getFileSetPath(securableObject)),
                              RangerMetadataObject.Type.PATH,
                              rangerPrivileges));
                      break;
                    default:
                      throw new AuthorizationPluginException(
                          "The privilege %s is not supported for the securable object: %s",
                          gravitinoPrivilege.name(), securableObject.type());
                  }
                  break;
                default:
                  LOG.warn(
                      "RangerAuthorizationHDFSPlugin -> privilege {} is not supported for the securable object: {}",
                      gravitinoPrivilege.name(),
                      securableObject.type());
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
        return rangerSecurableObjects;
      case FILESET:
        rangerSecurableObjects.add(
            generateAuthorizationSecurableObject(
                ImmutableList.of(getFileSetPath(gravitinoMetadataObject)),
                RangerMetadataObject.Type.PATH,
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
    Preconditions.checkArgument(
        !(metadataObject instanceof RangerPrivileges),
        "The metadata object must be not a RangerPrivileges object.");
    List<String> nsMetadataObject =
        Lists.newArrayList(SecurableObjects.DOT_SPLITTER.splitToList(metadataObject.fullName()));
    Preconditions.checkArgument(
        nsMetadataObject.size() > 0, "The metadata object must have at least one name.");

    RangerMetadataObject rangerMetadataObject =
        new RangerMetadataObject(
            AuthorizationMetadataObject.getParentFullName(nsMetadataObject),
            AuthorizationMetadataObject.getLastName(nsMetadataObject),
            RangerMetadataObject.Type.PATH);
    rangerMetadataObject.validateAuthorizationMetadataObject();
    return rangerMetadataObject;
  }

  private String getFileSetPath(MetadataObject metadataObject) {
    // TODO how to get metalake ?
    NameIdentifier identifier = NameIdentifier.parse("metalake." + metadataObject.fullName());
    Fileset fileset = GravitinoEnv.getInstance().filesetDispatcher().loadFileset(identifier);
    Preconditions.checkArgument(
        fileset != null, String.format("Fileset %s is not found", identifier));
    String filesetLocation = fileset.storageLocation();
    LOG.warn("getFileSetPath filesetLocation {}", filesetLocation);
    Preconditions.checkArgument(
        filesetLocation != null, String.format("Fileset %s location is not found", identifier));
    return pattern.matcher(filesetLocation).replaceAll("");
  }
}
