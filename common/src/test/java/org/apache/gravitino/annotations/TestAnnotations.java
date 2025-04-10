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

package org.apache.gravitino.annotations;

import java.lang.reflect.Method;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.authorization.Privilege;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestAnnotations {
  @Test
  void testAuthorizeApiWithResourceType() throws NoSuchMethodException {
    Class<TestClass> testClass = TestClass.class;
    Method method = testClass.getMethod("testAuthedMethodUseResourceType");

    boolean hasAnnotation = method.isAnnotationPresent(AuthorizeApi.class);
    Assertions.assertTrue(hasAnnotation);

    AuthorizeApi annotation = method.getAnnotation(AuthorizeApi.class);
    Assertions.assertNotNull(annotation);

    Assertions.assertArrayEquals(
        new Privilege.Name[] {Privilege.Name.CREATE_CATALOG, Privilege.Name.USE_CATALOG},
        annotation.privileges());
    Assertions.assertEquals(MetadataObject.Type.CATALOG, annotation.resourceType());
    Assertions.assertEquals("", annotation.expression());
    Assertions.assertEquals(AuthorizeApi.AuthorizeType.RESOURCE_TYPE, annotation.rule());
  }

  @Test
  void testAuthorizeApiWithExpression() throws NoSuchMethodException {
    Class<TestClass> testClass = TestClass.class;
    Method method = testClass.getMethod("testAuthedMethodUseExpression");

    boolean hasAnnotation = method.isAnnotationPresent(AuthorizeApi.class);
    Assertions.assertTrue(hasAnnotation);

    AuthorizeApi annotation = method.getAnnotation(AuthorizeApi.class);
    Assertions.assertNotNull(annotation);

    Assertions.assertArrayEquals(new Privilege.Name[] {}, annotation.privileges());
    Assertions.assertEquals(MetadataObject.Type.UNKNOWN, annotation.resourceType());
    Assertions.assertEquals(
        "CATALOG::CREATE_TABLE || TABLE::CREATE_TABLE", annotation.expression());
    Assertions.assertEquals(AuthorizeApi.AuthorizeType.EXPRESSION, annotation.rule());
  }
}

class TestClass {
  @AuthorizeApi(
      privileges = {Privilege.Name.CREATE_CATALOG, Privilege.Name.USE_CATALOG},
      resourceType = MetadataObject.Type.CATALOG)
  public void testAuthedMethodUseResourceType() {}

  @AuthorizeApi(
      expression = "CATALOG::CREATE_TABLE || TABLE::CREATE_TABLE",
      rule = AuthorizeApi.AuthorizeType.EXPRESSION)
  public void testAuthedMethodUseExpression() {}
}
