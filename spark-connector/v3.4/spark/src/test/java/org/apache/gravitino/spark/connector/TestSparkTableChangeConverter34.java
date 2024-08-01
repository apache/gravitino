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
package org.apache.gravitino.spark.connector;

import org.apache.gravitino.rel.expressions.literals.Literals;
import org.apache.spark.sql.connector.catalog.TableChange;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestSparkTableChangeConverter34 {

  private SparkTableChangeConverter sparkTableChangeConverter =
      new SparkTableChangeConverter34(new SparkTypeConverter34());

  @Test
  void testUpdateColumnDefaultValue() {
    String[] fieldNames = new String[] {"col"};
    String defaultValue = "col_default_value";
    TableChange.UpdateColumnDefaultValue sparkUpdateColumnDefaultValue =
        (TableChange.UpdateColumnDefaultValue)
            TableChange.updateColumnDefaultValue(fieldNames, defaultValue);

    org.apache.gravitino.rel.TableChange gravitinoChange =
        sparkTableChangeConverter.toGravitinoTableChange(sparkUpdateColumnDefaultValue);

    Assertions.assertTrue(
        gravitinoChange instanceof org.apache.gravitino.rel.TableChange.UpdateColumnDefaultValue);
    org.apache.gravitino.rel.TableChange.UpdateColumnDefaultValue
        gravitinoUpdateColumnDefaultValue =
            (org.apache.gravitino.rel.TableChange.UpdateColumnDefaultValue) gravitinoChange;

    Assertions.assertArrayEquals(
        sparkUpdateColumnDefaultValue.fieldNames(), gravitinoUpdateColumnDefaultValue.fieldName());
    Assertions.assertEquals(
        Literals.stringLiteral(defaultValue),
        gravitinoUpdateColumnDefaultValue.getNewDefaultValue());
  }
}
