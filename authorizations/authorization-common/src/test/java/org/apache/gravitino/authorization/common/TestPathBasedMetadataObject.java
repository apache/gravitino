package org.apache.gravitino.authorization.common;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestPathBasedMetadataObject {
  @Test
  public void PathBasedMetadataObjectEquals() {
    PathBasedMetadataObject pathBasedMetadataObject1 =
        new PathBasedMetadataObject("parent", "name", "path", PathBasedMetadataObject.Type.PATH);
    pathBasedMetadataObject1.validateAuthorizationMetadataObject();

    PathBasedMetadataObject pathBasedMetadataObject2 =
        new PathBasedMetadataObject("parent", "name", "path", PathBasedMetadataObject.Type.PATH);
    pathBasedMetadataObject2.validateAuthorizationMetadataObject();

    Assertions.assertEquals(pathBasedMetadataObject1, pathBasedMetadataObject2);
  }

  @Test
  public void PathBasedMetadataObjectNotEquals() {
    PathBasedMetadataObject pathBasedMetadataObject1 =
        new PathBasedMetadataObject("parent", "name", "path", PathBasedMetadataObject.Type.PATH);
    pathBasedMetadataObject1.validateAuthorizationMetadataObject();

    PathBasedMetadataObject pathBasedMetadataObject2 =
        new PathBasedMetadataObject("parent", "name", "path1", PathBasedMetadataObject.Type.PATH);
    pathBasedMetadataObject2.validateAuthorizationMetadataObject();

    Assertions.assertNotEquals(pathBasedMetadataObject1, pathBasedMetadataObject2);
  }
}
