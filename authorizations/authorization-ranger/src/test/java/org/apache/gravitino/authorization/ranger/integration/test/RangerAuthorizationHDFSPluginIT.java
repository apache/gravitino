package org.apache.gravitino.authorization.ranger.integration.test;

import com.google.common.collect.Lists;
import java.util.List;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.MetadataObjects;
import org.apache.gravitino.authorization.AuthorizationMetadataObject;
import org.apache.gravitino.authorization.AuthorizationSecurableObject;
import org.apache.gravitino.authorization.Privileges;
import org.apache.gravitino.authorization.SecurableObject;
import org.apache.gravitino.authorization.SecurableObjects;
import org.apache.gravitino.authorization.ranger.RangerAuthorizationPlugin;
import org.apache.gravitino.authorization.ranger.RangerMetadataObject;
import org.apache.gravitino.authorization.ranger.RangerPrivileges;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag("gravitino-docker-test")
public class RangerAuthorizationHDFSPluginIT {

  private static RangerAuthorizationPlugin rangerAuthPlugin;

  @BeforeAll
  public static void setup() {
    RangerITEnv.init();
    rangerAuthPlugin = RangerITEnv.rangerAuthHDFSPlugin;
  }

  @Test
  public void testTranslateMetadataObject() {
    MetadataObject metalake =
        MetadataObjects.parse(String.format("metalake1"), MetadataObject.Type.METALAKE);
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> rangerAuthPlugin.translateMetadataObject(metalake));

    MetadataObject catalog =
        MetadataObjects.parse(String.format("catalog1"), MetadataObject.Type.CATALOG);
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> rangerAuthPlugin.translateMetadataObject(catalog));

    MetadataObject schema =
        MetadataObjects.parse(String.format("catalog1.schema1"), MetadataObject.Type.SCHEMA);
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> rangerAuthPlugin.translateMetadataObject(schema));

    MetadataObject table =
        MetadataObjects.parse(String.format("catalog1.schema1.tab1"), MetadataObject.Type.TABLE);
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> rangerAuthPlugin.translateMetadataObject(table));

    MetadataObject fileset =
        MetadataObjects.parse(
            String.format("catalog1.schema1.fileset1"), MetadataObject.Type.FILESET);
    AuthorizationMetadataObject rangerFileset = rangerAuthPlugin.translateMetadataObject(fileset);
    Assertions.assertEquals(2, rangerFileset.names().size());
    Assertions.assertEquals("schema1", rangerFileset.names().get(0));
    Assertions.assertEquals("fileset1", rangerFileset.names().get(1));
    Assertions.assertEquals(RangerMetadataObject.Type.TABLE, rangerFileset.type());
  }

  @Test
  public void testTranslatePrivilege() {
    SecurableObject filesetInMetalake =
        SecurableObjects.parse(
            String.format("metalake1"),
            MetadataObject.Type.METALAKE,
            Lists.newArrayList(
                Privileges.CreateFileset.allow(),
                Privileges.ReadFileset.allow(),
                Privileges.WriteFileset.allow()));
    List<AuthorizationSecurableObject> filesetInMetalake1 =
        rangerAuthPlugin.translatePrivilege(filesetInMetalake);
    Assertions.assertEquals(0, filesetInMetalake1.size());

    SecurableObject filesetInCatalog =
        SecurableObjects.parse(
            String.format("catalog1"),
            MetadataObject.Type.CATALOG,
            Lists.newArrayList(
                Privileges.CreateFileset.allow(),
                Privileges.ReadFileset.allow(),
                Privileges.WriteFileset.allow()));
    List<AuthorizationSecurableObject> filesetInCatalog1 =
        rangerAuthPlugin.translatePrivilege(filesetInCatalog);
    Assertions.assertEquals(0, filesetInCatalog1.size());

    SecurableObject filesetInSchema =
        SecurableObjects.parse(
            String.format("catalog1.schema1"),
            MetadataObject.Type.SCHEMA,
            Lists.newArrayList(
                Privileges.CreateFileset.allow(),
                Privileges.ReadFileset.allow(),
                Privileges.WriteFileset.allow()));
    List<AuthorizationSecurableObject> filesetInSchema1 =
        rangerAuthPlugin.translatePrivilege(filesetInSchema);
    Assertions.assertEquals(0, filesetInSchema1.size());

    SecurableObject filesetInFileset =
        SecurableObjects.parse(
            String.format("catalog1.schema1.fileset1"),
            MetadataObject.Type.FILESET,
            Lists.newArrayList(
                Privileges.CreateFileset.allow(),
                Privileges.ReadFileset.allow(),
                Privileges.WriteFileset.allow()));
    List<AuthorizationSecurableObject> filesetInFileset1 =
        rangerAuthPlugin.translatePrivilege(filesetInFileset);
    Assertions.assertEquals(2, filesetInSchema1.size());
    Assertions.assertEquals("catalog1.schema1.fileset1", filesetInFileset1.get(0).fullName());
    Assertions.assertEquals(RangerMetadataObject.Type.PATH, filesetInFileset1.get(0).type());
    filesetInFileset1
        .get(0)
        .privileges()
        .forEach(
            privilege ->
                Assertions.assertEquals(
                    RangerPrivileges.RangerHdfsPrivilege.READ.getName(), privilege.getName()));
    Assertions.assertEquals("catalog1.schema1.fileset1", filesetInFileset1.get(1).fullName());
    Assertions.assertEquals(RangerMetadataObject.Type.PATH, filesetInFileset1.get(1).type());
    Assertions.assertEquals(2, filesetInFileset1.get(1).privileges().size());
  }

  @Test
  public void testTranslateOwner() {
    MetadataObject metalake =
        MetadataObjects.parse(String.format("metalake1"), MetadataObject.Type.METALAKE);
    List<AuthorizationSecurableObject> metalakeOwner = rangerAuthPlugin.translateOwner(metalake);
    Assertions.assertEquals(0, metalakeOwner.size());

    MetadataObject catalog =
        MetadataObjects.parse(String.format("catalog1"), MetadataObject.Type.CATALOG);
    List<AuthorizationSecurableObject> catalogOwner = rangerAuthPlugin.translateOwner(catalog);
    Assertions.assertEquals(0, catalogOwner.size());

    MetadataObject schema =
        MetadataObjects.parse(String.format("catalog1.schema1"), MetadataObject.Type.SCHEMA);
    List<AuthorizationSecurableObject> schemaOwner = rangerAuthPlugin.translateOwner(schema);
    Assertions.assertEquals(0, schemaOwner.size());

    MetadataObject fileset =
        MetadataObjects.parse(
            String.format("catalog1.schema1.fileset1"), MetadataObject.Type.FILESET);
    List<AuthorizationSecurableObject> filesetOwner = rangerAuthPlugin.translateOwner(fileset);
    Assertions.assertEquals(1, filesetOwner.size());
    Assertions.assertEquals("catalog1.schema1.fileset1", filesetOwner.get(0).fullName());
    Assertions.assertEquals(RangerMetadataObject.Type.PATH, filesetOwner.get(0).type());
    Assertions.assertEquals(3, filesetOwner.get(0).privileges().size());
  }
}
