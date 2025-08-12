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
package org.apache.gravitino.dto.rel.indexes;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestIndexDTO {

  @Test
  public void testHashCodeConsistentWithEquals() {
    String[][] fields1 = new String[][] {{"a"}, {"b"}};
    String[][] fields2 = new String[][] {{"a"}, {"b"}};

    IndexDTO index1 =
        IndexDTO.builder()
            .withIndexType(IndexDTO.IndexType.PRIMARY_KEY)
            .withName("idx")
            .withFieldNames(fields1)
            .build();

    IndexDTO index2 =
        IndexDTO.builder()
            .withIndexType(IndexDTO.IndexType.PRIMARY_KEY)
            .withName("idx")
            .withFieldNames(fields2)
            .build();

    Assertions.assertEquals(index1, index2);
    Assertions.assertEquals(index1.hashCode(), index2.hashCode());
  }
}
