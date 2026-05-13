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

import java.util.List;
import org.apache.gravitino.storage.relational.TestJDBCBackend;
import org.apache.gravitino.storage.relational.po.IdpGroupPO;
import org.apache.gravitino.storage.relational.po.IdpGroupUserRelPO;
import org.apache.gravitino.storage.relational.po.IdpUserPO;
import org.apache.gravitino.storage.relational.session.SqlSessionFactoryHelper;
import org.apache.ibatis.session.SqlSession;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

abstract class IdpMapperTestBase extends TestJDBCBackend {
  protected SqlSession sharedSession;
  protected IdpUserMetaMapper idpUserMetaMapper;
  protected IdpGroupMetaMapper idpGroupMetaMapper;
  protected IdpGroupUserRelMapper idpGroupUserRelMapper;

  @BeforeEach
  void openSession() {
    sharedSession = SqlSessionFactoryHelper.getInstance().getSqlSessionFactory().openSession(true);
    idpUserMetaMapper = sharedSession.getMapper(IdpUserMetaMapper.class);
    idpGroupMetaMapper = sharedSession.getMapper(IdpGroupMetaMapper.class);
    idpGroupUserRelMapper = sharedSession.getMapper(IdpGroupUserRelMapper.class);
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

  protected IdpGroupPO insertGroup(
      long groupId, String groupName, long currentVersion, long lastVersion, long deletedAt) {
    IdpGroupPO groupPO =
        IdpGroupPO.builder()
            .withGroupId(groupId)
            .withGroupName(groupName)
            .withCurrentVersion(currentVersion)
            .withLastVersion(lastVersion)
            .withDeletedAt(deletedAt)
            .build();
    idpGroupMetaMapper.insertIdpGroup(groupPO);
    return groupPO;
  }

  protected IdpGroupUserRelPO insertRelation(
      long id, long groupId, long userId, long currentVersion, long lastVersion, long deletedAt) {
    IdpGroupUserRelPO relationPO =
        IdpGroupUserRelPO.builder()
            .withId(id)
            .withGroupId(groupId)
            .withUserId(userId)
            .withCurrentVersion(currentVersion)
            .withLastVersion(lastVersion)
            .withDeletedAt(deletedAt)
            .build();
    idpGroupUserRelMapper.batchInsertIdpGroupUsers(List.of(relationPO));
    return relationPO;
  }
}
