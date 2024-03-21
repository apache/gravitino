/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.rel;

import com.datastrato.gravitino.rel.types.Types;
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
