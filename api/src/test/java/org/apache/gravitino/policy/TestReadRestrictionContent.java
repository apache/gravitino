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
package org.apache.gravitino.policy;

import com.google.common.collect.ImmutableSet;
import java.util.Map;
import org.apache.gravitino.MetadataObject;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestReadRestrictionContent {

  @Test
  void testCreatesReadRestrictionContent() {
    String rowFilterExpression =
        "filter := col(\"region\") == \"US\" if is_group_member(\"analysts\") else := false";
    String columnMaskExpression =
        "mask := action(\"show-last-4\") if is_group_member(\"analysts\") "
            + "else := action(\"replace-with-null\")";

    RowFilterContent rowFilter = PolicyContents.rowFilter(rowFilterExpression);
    ColumnMaskContent columnMask = PolicyContents.columnMask(columnMaskExpression);

    Assertions.assertEquals(rowFilterExpression, rowFilter.expression());
    Assertions.assertEquals(columnMaskExpression, columnMask.expression());
    Assertions.assertEquals(Map.of("expression", rowFilterExpression), rowFilter.rules());
    Assertions.assertEquals(Map.of("expression", columnMaskExpression), columnMask.rules());
    Assertions.assertEquals(
        ImmutableSet.of(MetadataObject.Type.TABLE), rowFilter.supportedObjectTypes());
    Assertions.assertEquals(
        ImmutableSet.of(MetadataObject.Type.TABLE, MetadataObject.Type.COLUMN),
        columnMask.supportedObjectTypes());
    Assertions.assertTrue(rowFilter.properties().isEmpty());
    Assertions.assertTrue(columnMask.properties().isEmpty());
    Assertions.assertDoesNotThrow(rowFilter::validate);
    Assertions.assertDoesNotThrow(columnMask::validate);
  }

  @Test
  void testEqualityUsesPolicyTypeAndExpression() {
    String expression = "filter := col(\"region\") == \"US\"";

    Assertions.assertEquals(
        PolicyContents.rowFilter(expression), PolicyContents.rowFilter(expression));
    Assertions.assertNotEquals(
        PolicyContents.rowFilter(expression), PolicyContents.rowFilter("filter := true"));
    Assertions.assertNotEquals(
        PolicyContents.rowFilter(expression), PolicyContents.columnMask(expression));
  }

  @Test
  void testRejectsMissingOrBlankExpressions() {
    Assertions.assertThrows(IllegalArgumentException.class, () -> PolicyContents.rowFilter(null));
    Assertions.assertThrows(IllegalArgumentException.class, () -> PolicyContents.columnMask(null));
    Assertions.assertThrows(IllegalArgumentException.class, () -> PolicyContents.rowFilter(" \t"));
    Assertions.assertThrows(IllegalArgumentException.class, () -> PolicyContents.columnMask("\n"));
  }

  @Test
  void testValidatesUtf8ExpressionLength() {
    Assertions.assertEquals(16 * 1024, ReadRestrictionContent.MAX_SOURCE_LENGTH_BYTES);

    String maximumLength = "é".repeat(ReadRestrictionContent.MAX_SOURCE_LENGTH_BYTES / 2);
    String tooLong = maximumLength + "a";

    Assertions.assertDoesNotThrow(() -> PolicyContents.rowFilter(maximumLength));
    Assertions.assertDoesNotThrow(() -> PolicyContents.columnMask(maximumLength));
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> PolicyContents.rowFilter(tooLong));
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> PolicyContents.columnMask(tooLong));
  }
}
