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
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.NameIdentifier;
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

  private RangerAuthorizationHDFSPlugin(Map<String, String> config) {
    super(config);
  }

  public static synchronized RangerAuthorizationHDFSPlugin getInstance(Map<String, String> config) {
    return new RangerAuthorizationHDFSPlugin(config);
  }

  @Override
  public void validateRangerMetadataObject(List<String> names, RangerMetadataObject.Type type)
      throws IllegalArgumentException {
    Preconditions.checkArgument(
        names != null && !names.isEmpty(), "Cannot create a Ranger metadata object with no names");
    Preconditions.checkArgument(
        names.size() != 1,
        "Cannot create a Ranger metadata object with the name length which is greater than 3");
    Preconditions.checkArgument(
        type != RangerMetadataObject.Type.PATH,
        "Cannot create a Ranger metadata object with no type");

    for (String name : names) {
      RangerMetadataObjects.checkName(name);
    }
  }

  @Override
  public Map<Privilege.Name, Set<RangerPrivilege>> privilegesMappingRule() {
    return ImmutableMap.of(
        Privilege.Name.READ_FILESET,
        ImmutableSet.of(RangerPrivileges.RangerHdfsPrivilege.READ),
        Privilege.Name.WRITE_FILESET,
        ImmutableSet.of(RangerPrivileges.RangerHdfsPrivilege.WRITE));
  }

  @Override
  public Set<RangerPrivilege> ownerMappingRule() {
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
                case CREATE_FILESET:
                  // Ignore the Gravitino privilege `CREATE_FILESET` in the
                  // RangerAuthorizationHDFSPlugin
                  break;
                case READ_FILESET:
                case WRITE_FILESET:
                  switch (securableObject.type()) {
                    case FILESET:
                      rangerSecurableObjects.add(
                          generateRangerSecurableObject(
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
  public List<RangerSecurableObject> translateOwner(MetadataObject gravitinoMetadataObject) {
    List<RangerSecurableObject> rangerSecurableObjects = new ArrayList<>();

    switch (gravitinoMetadataObject.type()) {
      case FILESET:
        rangerSecurableObjects.add(
            generateRangerSecurableObject(
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
  public RangerMetadataObject translateMetadataObject(MetadataObject metadataObject) {
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

    return new RangerMetadataObjects.RangerMetadataObjectImpl(
        null, "location", RangerMetadataObject.Type.PATH);
  }

  private String getFileSetPath(MetadataObject metadataObject) {
    NameIdentifier identifier = NameIdentifier.of(metadataObject.parent(), metadataObject.name());
    Fileset fileset = GravitinoEnv.getInstance().filesetDispatcher().loadFileset(identifier);
    Preconditions.checkArgument(
        fileset != null, String.format("Fileset %s is not found", identifier));
    return fileset.storageLocation();
  }
}
