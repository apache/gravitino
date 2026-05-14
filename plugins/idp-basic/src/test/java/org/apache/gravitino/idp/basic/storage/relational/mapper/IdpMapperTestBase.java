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

package org.apache.gravitino.idp.basic.storage.relational.mapper;

import org.apache.gravitino.idp.basic.storage.relational.TestJDBCBackend;
import org.apache.gravitino.idp.basic.storage.relational.po.IdpUserPO;
import org.apache.gravitino.storage.relational.session.SqlSessionFactoryHelper;
import org.apache.ibatis.session.SqlSession;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

abstract class IdpMapperTestBase extends TestJDBCBackend {
  protected SqlSession sharedSession;
  protected IdpUserMetaMapper idpUserMetaMapper;

  @BeforeEach
  void openSession() {
    sharedSession = SqlSessionFactoryHelper.getInstance().getSqlSessionFactory().openSession(true);
    idpUserMetaMapper = sharedSession.getMapper(IdpUserMetaMapper.class);
  }

  @AfterEach
  void closeSession() {
    if (sharedSession != null) {
      sharedSession.close();
      sharedSession = null;
    }
  }

  protected IdpUserPO insertUser(
      long userId,
      String userName,
      String passwordHash,
      long currentVersion,
      long lastVersion,
      long deletedAt) {
    IdpUserPO userPO =
        IdpUserPO.builder()
            .withUserId(userId)
            .withUserName(userName)
            .withPasswordHash(passwordHash)
            .withCurrentVersion(currentVersion)
            .withLastVersion(lastVersion)
            .withDeletedAt(deletedAt)
            .build();
    idpUserMetaMapper.insertIdpUser(userPO);
    return userPO;
  }

  long queryLongValueInMapperTest(String table, String column, String idColumn, long idValue) {
    return queryLongValue(table, column, idColumn, idValue);
  }

  int countRowsInMapperTest(String table, String idColumn, long idValue) {
    return countRows(table, idColumn, idValue);
  }
}
