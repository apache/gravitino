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
package org.apache.gravitino.rel;

import org.apache.gravitino.rel.types.Types;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestColumn {
  @Test
  public void testColumn() {
    Column expectedColumn =
        Column.of("col_1", Types.ByteType.get(), null, true, false, Column.DEFAULT_VALUE_NOT_SET);

    Column actualColumn = Column.of("col_1", Types.ByteType.get());
    Assertions.assertEquals(expectedColumn, actualColumn);

    actualColumn = Column.of("col_1", Types.ByteType.get(), null);
    Assertions.assertEquals(expectedColumn, actualColumn);
  }

  @Test
  public void testColumnException() {
    IllegalArgumentException exception =
        Assertions.assertThrows(IllegalArgumentException.class, () -> Column.of(null, null));
    Assertions.assertEquals("Column name cannot be null", exception.getMessage());

    exception =
        Assertions.assertThrows(IllegalArgumentException.class, () -> Column.of("col_1", null));
    Assertions.assertEquals("Column data type cannot be null", exception.getMessage());
  }
}
