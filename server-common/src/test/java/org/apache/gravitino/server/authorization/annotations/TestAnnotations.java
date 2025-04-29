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

package org.apache.gravitino.server.authorization.annotations;

import java.lang.reflect.Method;
import java.lang.reflect.Parameter;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.authorization.Privilege;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestAnnotations {

  // This class is used to test the AuthorizeResource annotation.
  static class TestResourceAnnotationClass {

    public void methodWithAnnotatedParam(
        @AuthorizationMetadata(type = MetadataObject.Type.TABLE) String table) {
      // dummy method
    }

    public void listSchemas(
        @AuthorizationMetadata(type = MetadataObject.Type.METALAKE) String metalake,
        @AuthorizationMetadata(type = MetadataObject.Type.CATALOG) String catalog) {
      // dummy method
    }
  }

  // This class is used to test the AuthorizeApi annotation.
  // 1. ResourceAuthorizeApi
  // 2. AuthorizationExpression
  static class TestAuthorizeAnnotationClass {
    @AuthorizationMetadataPrivileges(
        privileges = {Privilege.Name.CREATE_CATALOG, Privilege.Name.USE_CATALOG},
        metadataType = MetadataObject.Type.CATALOG)
    public void testAuthedMethodUseResourceType() {}

    @AuthorizationExpression(expression = "CATALOG::CREATE_TABLE || TABLE::CREATE_TABLE")
    public void testAuthedMethodUseExpression() {}
  }

  @Test
  void testAuthorizeApiWithResourceType() throws NoSuchMethodException {
    Class<TestAuthorizeAnnotationClass> testClass = TestAuthorizeAnnotationClass.class;
    Method method = testClass.getMethod("testAuthedMethodUseResourceType");

    boolean hasAnnotation = method.isAnnotationPresent(AuthorizationMetadataPrivileges.class);
    Assertions.assertTrue(hasAnnotation);

    AuthorizationMetadataPrivileges annotation =
        method.getAnnotation(AuthorizationMetadataPrivileges.class);
    Assertions.assertNotNull(annotation);

    Assertions.assertArrayEquals(
        new Privilege.Name[] {Privilege.Name.CREATE_CATALOG, Privilege.Name.USE_CATALOG},
        annotation.privileges());
    Assertions.assertEquals(MetadataObject.Type.CATALOG, annotation.metadataType());
  }

  @Test
  void testAuthorizeApiWithExpression() throws NoSuchMethodException {
    Class<TestAuthorizeAnnotationClass> testClass = TestAuthorizeAnnotationClass.class;
    Method method = testClass.getMethod("testAuthedMethodUseExpression");

    boolean hasAnnotation = method.isAnnotationPresent(AuthorizationExpression.class);
    Assertions.assertTrue(hasAnnotation);

    AuthorizationExpression annotation = method.getAnnotation(AuthorizationExpression.class);
    Assertions.assertNotNull(annotation);

    Assertions.assertEquals(
        "CATALOG::CREATE_TABLE || TABLE::CREATE_TABLE", annotation.expression());
  }

  @Test
  void testParameterAnnotationPresent() throws NoSuchMethodException {
    Parameter argument =
        TestResourceAnnotationClass.class.getMethod("methodWithAnnotatedParam", String.class)
            .getParameters()[0];
    AuthorizationMetadata annotation = argument.getAnnotation(AuthorizationMetadata.class);
    Assertions.assertNotNull(annotation);
    Assertions.assertEquals(MetadataObject.Type.TABLE, annotation.type());
  }

  @Test
  void testAnnotateListSchemas() throws NoSuchMethodException {
    Parameter[] arguments =
        TestResourceAnnotationClass.class
            .getMethod("listSchemas", String.class, String.class)
            .getParameters();

    Parameter argumentMetalake = arguments[0];
    AuthorizationMetadata metalakeAnnotation =
        argumentMetalake.getAnnotation(AuthorizationMetadata.class);
    Assertions.assertNotNull(metalakeAnnotation);
    Assertions.assertEquals(MetadataObject.Type.METALAKE, metalakeAnnotation.type());

    Parameter argumentCatalog = arguments[1];
    AuthorizationMetadata catalogAnnotation =
        argumentCatalog.getAnnotation(AuthorizationMetadata.class);
    Assertions.assertNotNull(catalogAnnotation);
    Assertions.assertEquals(MetadataObject.Type.CATALOG, catalogAnnotation.type());
  }
}
