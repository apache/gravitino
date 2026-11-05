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
package org.apache.gravitino.storage.relational.mapper;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import org.apache.ibatis.annotations.Param;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class TestRoleMetaMapper {

  @Test
  void testSoftDeleteRoleMetaByRoleIdHasNamedParam() throws NoSuchMethodException {
    Method method =
        RoleMetaMapper.class.getMethod("softDeleteRoleMetaByRoleId", Long.class, Long.class);
    Annotation[][] parameterAnnotations = method.getParameterAnnotations();

    Assertions.assertEquals(2, parameterAnnotations.length);

    Param roleIdParam = null;
    for (Annotation annotation : parameterAnnotations[0]) {
      if (annotation instanceof Param) {
        roleIdParam = (Param) annotation;
        break;
      }
    }

    Assertions.assertNotNull(
        roleIdParam,
        "Missing @Param on softDeleteRoleMetaByRoleId may break MyBatis named binding.");
    Assertions.assertEquals("roleId", roleIdParam.value());
  }
}
