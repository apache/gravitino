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

package org.apache.gravitino.idp.storage.mapper.provider.postgresql;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestIdpUserMetaPostgreSQLProvider {

  @Test
  void testCurrentTimeMillisExpression() {
    IdpUserMetaPostgreSQLProvider provider = new IdpUserMetaPostgreSQLProvider();

    Assertions.assertEquals(
        "CAST(EXTRACT(EPOCH FROM CURRENT_TIMESTAMP) * 1000 AS BIGINT)",
        provider.currentTimeMillisExpression());
  }

  @Test
  void testDeleteIdpUserMetasByLegacyTimeline() {
    IdpUserMetaPostgreSQLProvider provider = new IdpUserMetaPostgreSQLProvider();

    Assertions.assertEquals(
        "DELETE FROM idp_user_meta WHERE user_id IN (SELECT user_id FROM idp_user_meta"
            + " WHERE deleted_at > 0 AND deleted_at < #{legacyTimeline} LIMIT #{limit})",
        provider.deleteIdpUserMetasByLegacyTimeline(1L, 2));
  }
}
