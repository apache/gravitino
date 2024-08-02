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

package org.apache.gravitino.spark.connector.catalog;

import org.apache.gravitino.rel.TableChange.UpdateComment;
import org.apache.gravitino.spark.connector.ConnectorConstants;
import org.apache.gravitino.spark.connector.SparkTableChangeConverter;
import org.apache.gravitino.spark.connector.SparkTypeConverter;
import org.apache.spark.sql.connector.catalog.TableChange;
import org.apache.spark.sql.types.DataTypes;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestTransformTableChange {
  private SparkTableChangeConverter sparkTableChangeConverter =
      new SparkTableChangeConverter(new SparkTypeConverter());

  @Test
  void testTransformSetProperty() {
    TableChange sparkSetProperty = TableChange.setProperty("key", "value");
    org.apache.gravitino.rel.TableChange tableChange =
        sparkTableChangeConverter.toGravitinoTableChange(sparkSetProperty);
    Assertions.assertTrue(tableChange instanceof org.apache.gravitino.rel.TableChange.SetProperty);
    org.apache.gravitino.rel.TableChange.SetProperty gravitinoSetProperty =
        (org.apache.gravitino.rel.TableChange.SetProperty) tableChange;
    Assertions.assertEquals("key", gravitinoSetProperty.getProperty());
    Assertions.assertEquals("value", gravitinoSetProperty.getValue());
  }

  @Test
  void testTransformRemoveProperty() {
    TableChange sparkRemoveProperty = TableChange.removeProperty("key");
    org.apache.gravitino.rel.TableChange tableChange =
        sparkTableChangeConverter.toGravitinoTableChange(sparkRemoveProperty);
    Assertions.assertTrue(
        tableChange instanceof org.apache.gravitino.rel.TableChange.RemoveProperty);
    org.apache.gravitino.rel.TableChange.RemoveProperty gravitinoRemoveProperty =
        (org.apache.gravitino.rel.TableChange.RemoveProperty) tableChange;
    Assertions.assertEquals("key", gravitinoRemoveProperty.getProperty());
  }

  @Test
  void testTransformUpdateComment() {
    TableChange sparkSetProperty = TableChange.setProperty(ConnectorConstants.COMMENT, "a");
    org.apache.gravitino.rel.TableChange tableChange =
        sparkTableChangeConverter.toGravitinoTableChange(sparkSetProperty);
    Assertions.assertTrue(
        tableChange instanceof org.apache.gravitino.rel.TableChange.UpdateComment);
    Assertions.assertEquals("a", ((UpdateComment) tableChange).getNewComment());

    TableChange sparkRemoveProperty = TableChange.removeProperty(ConnectorConstants.COMMENT);
    Assertions.assertThrowsExactly(
        IllegalArgumentException.class,
        () -> sparkTableChangeConverter.toGravitinoTableChange(sparkRemoveProperty));
  }

  @Test
  void testTransformRenameColumn() {
    String[] oldFieldsName = new String[] {"default_name"};
    String newFiledName = "new_name";

    TableChange.RenameColumn sparkRenameColumn =
        (TableChange.RenameColumn) TableChange.renameColumn(oldFieldsName, newFiledName);
    org.apache.gravitino.rel.TableChange gravitinoChange =
        sparkTableChangeConverter.toGravitinoTableChange(sparkRenameColumn);

    Assertions.assertTrue(
        gravitinoChange instanceof org.apache.gravitino.rel.TableChange.RenameColumn);
    org.apache.gravitino.rel.TableChange.RenameColumn gravitinoRenameColumn =
        (org.apache.gravitino.rel.TableChange.RenameColumn) gravitinoChange;

    Assertions.assertArrayEquals(oldFieldsName, gravitinoRenameColumn.getFieldName());
    Assertions.assertEquals(newFiledName, gravitinoRenameColumn.getNewName());
  }

  @Test
  void testTransformUpdateColumnComment() {
    String[] fieldNames = new String[] {"default_name"};
    String newComment = "default_comment";

    TableChange.UpdateColumnComment updateColumnComment =
        (TableChange.UpdateColumnComment) TableChange.updateColumnComment(fieldNames, newComment);
    org.apache.gravitino.rel.TableChange gravitinoChange =
        sparkTableChangeConverter.toGravitinoTableChange(updateColumnComment);

    Assertions.assertTrue(
        gravitinoChange instanceof org.apache.gravitino.rel.TableChange.UpdateColumnComment);
    org.apache.gravitino.rel.TableChange.UpdateColumnComment gravitinoUpdateColumnComment =
        (org.apache.gravitino.rel.TableChange.UpdateColumnComment) gravitinoChange;

    Assertions.assertArrayEquals(fieldNames, gravitinoUpdateColumnComment.getFieldName());
    Assertions.assertEquals(newComment, gravitinoUpdateColumnComment.getNewComment());
  }

  @Test
  void testTransformAddColumn() {

    TableChange.ColumnPosition first = TableChange.ColumnPosition.first();
    TableChange.ColumnPosition after = TableChange.ColumnPosition.after("col0");

    TableChange.AddColumn sparkAddColumnFirst =
        (TableChange.AddColumn)
            TableChange.addColumn(new String[] {"col1"}, DataTypes.StringType, true, "", first);
    org.apache.gravitino.rel.TableChange gravitinoChangeFirst =
        sparkTableChangeConverter.toGravitinoTableChange(sparkAddColumnFirst);

    Assertions.assertTrue(
        gravitinoChangeFirst instanceof org.apache.gravitino.rel.TableChange.AddColumn);
    org.apache.gravitino.rel.TableChange.AddColumn gravitinoAddColumnFirst =
        (org.apache.gravitino.rel.TableChange.AddColumn) gravitinoChangeFirst;

    Assertions.assertArrayEquals(
        sparkAddColumnFirst.fieldNames(), gravitinoAddColumnFirst.fieldName());
    Assertions.assertTrue(
        "string".equalsIgnoreCase(gravitinoAddColumnFirst.getDataType().simpleString()));
    Assertions.assertTrue(
        gravitinoAddColumnFirst.getPosition()
            instanceof org.apache.gravitino.rel.TableChange.First);

    TableChange.AddColumn sparkAddColumnAfter =
        (TableChange.AddColumn)
            TableChange.addColumn(new String[] {"col1"}, DataTypes.StringType, true, "", after);
    org.apache.gravitino.rel.TableChange gravitinoChangeAfter =
        sparkTableChangeConverter.toGravitinoTableChange(sparkAddColumnAfter);

    Assertions.assertTrue(
        gravitinoChangeAfter instanceof org.apache.gravitino.rel.TableChange.AddColumn);
    org.apache.gravitino.rel.TableChange.AddColumn gravitinoAddColumnAfter =
        (org.apache.gravitino.rel.TableChange.AddColumn) gravitinoChangeAfter;

    Assertions.assertArrayEquals(
        sparkAddColumnAfter.fieldNames(), gravitinoAddColumnAfter.fieldName());
    Assertions.assertTrue(
        "string".equalsIgnoreCase(gravitinoAddColumnAfter.getDataType().simpleString()));
    Assertions.assertTrue(
        gravitinoAddColumnAfter.getPosition()
            instanceof org.apache.gravitino.rel.TableChange.After);

    TableChange.AddColumn sparkAddColumnDefault =
        (TableChange.AddColumn)
            TableChange.addColumn(new String[] {"col1"}, DataTypes.StringType, true, "", null);
    org.apache.gravitino.rel.TableChange gravitinoChangeDefault =
        sparkTableChangeConverter.toGravitinoTableChange(sparkAddColumnDefault);

    Assertions.assertTrue(
        gravitinoChangeDefault instanceof org.apache.gravitino.rel.TableChange.AddColumn);
    org.apache.gravitino.rel.TableChange.AddColumn gravitinoAddColumnDefault =
        (org.apache.gravitino.rel.TableChange.AddColumn) gravitinoChangeDefault;

    Assertions.assertArrayEquals(
        sparkAddColumnDefault.fieldNames(), gravitinoAddColumnDefault.fieldName());
    Assertions.assertTrue(
        "string".equalsIgnoreCase(gravitinoAddColumnDefault.getDataType().simpleString()));
    Assertions.assertTrue(
        gravitinoAddColumnDefault.getPosition()
            instanceof org.apache.gravitino.rel.TableChange.Default);
  }

  @Test
  void testTransformDeleteColumn() {
    TableChange.DeleteColumn sparkDeleteColumn =
        (TableChange.DeleteColumn) TableChange.deleteColumn(new String[] {"col1"}, true);
    org.apache.gravitino.rel.TableChange gravitinoChange =
        sparkTableChangeConverter.toGravitinoTableChange(sparkDeleteColumn);

    Assertions.assertTrue(
        gravitinoChange instanceof org.apache.gravitino.rel.TableChange.DeleteColumn);
    org.apache.gravitino.rel.TableChange.DeleteColumn gravitinoDeleteColumn =
        (org.apache.gravitino.rel.TableChange.DeleteColumn) gravitinoChange;

    Assertions.assertArrayEquals(sparkDeleteColumn.fieldNames(), gravitinoDeleteColumn.fieldName());
    Assertions.assertEquals(sparkDeleteColumn.ifExists(), gravitinoDeleteColumn.getIfExists());
  }

  @Test
  void testTransformUpdateColumnType() {
    TableChange.UpdateColumnType sparkUpdateColumnType =
        (TableChange.UpdateColumnType)
            TableChange.updateColumnType(new String[] {"col1"}, DataTypes.StringType);
    org.apache.gravitino.rel.TableChange gravitinoChange =
        sparkTableChangeConverter.toGravitinoTableChange(sparkUpdateColumnType);

    Assertions.assertTrue(
        gravitinoChange instanceof org.apache.gravitino.rel.TableChange.UpdateColumnType);
    org.apache.gravitino.rel.TableChange.UpdateColumnType gravitinoUpdateColumnType =
        (org.apache.gravitino.rel.TableChange.UpdateColumnType) gravitinoChange;

    Assertions.assertArrayEquals(
        sparkUpdateColumnType.fieldNames(), gravitinoUpdateColumnType.fieldName());
    Assertions.assertTrue(
        "string".equalsIgnoreCase(gravitinoUpdateColumnType.getNewDataType().simpleString()));
  }

  @Test
  void testTransformUpdateColumnPosition() {
    TableChange.ColumnPosition first = TableChange.ColumnPosition.first();
    TableChange.ColumnPosition after = TableChange.ColumnPosition.after("col0");

    TableChange.UpdateColumnPosition sparkUpdateColumnFirst =
        (TableChange.UpdateColumnPosition)
            TableChange.updateColumnPosition(new String[] {"col1"}, first);
    org.apache.gravitino.rel.TableChange gravitinoChangeFirst =
        sparkTableChangeConverter.toGravitinoTableChange(sparkUpdateColumnFirst);

    Assertions.assertTrue(
        gravitinoChangeFirst instanceof org.apache.gravitino.rel.TableChange.UpdateColumnPosition);
    org.apache.gravitino.rel.TableChange.UpdateColumnPosition gravitinoUpdateColumnFirst =
        (org.apache.gravitino.rel.TableChange.UpdateColumnPosition) gravitinoChangeFirst;

    Assertions.assertArrayEquals(
        sparkUpdateColumnFirst.fieldNames(), gravitinoUpdateColumnFirst.fieldName());
    Assertions.assertTrue(
        gravitinoUpdateColumnFirst.getPosition()
            instanceof org.apache.gravitino.rel.TableChange.First);

    TableChange.UpdateColumnPosition sparkUpdateColumnAfter =
        (TableChange.UpdateColumnPosition)
            TableChange.updateColumnPosition(new String[] {"col1"}, after);
    org.apache.gravitino.rel.TableChange gravitinoChangeAfter =
        sparkTableChangeConverter.toGravitinoTableChange(sparkUpdateColumnAfter);

    Assertions.assertTrue(
        gravitinoChangeAfter instanceof org.apache.gravitino.rel.TableChange.UpdateColumnPosition);
    org.apache.gravitino.rel.TableChange.UpdateColumnPosition gravitinoUpdateColumnAfter =
        (org.apache.gravitino.rel.TableChange.UpdateColumnPosition) gravitinoChangeAfter;

    Assertions.assertArrayEquals(
        sparkUpdateColumnAfter.fieldNames(), gravitinoUpdateColumnAfter.fieldName());
    Assertions.assertTrue(
        gravitinoUpdateColumnAfter.getPosition()
            instanceof org.apache.gravitino.rel.TableChange.After);
  }

  @Test
  void testTransformUpdateColumnNullability() {
    TableChange.UpdateColumnNullability sparkUpdateColumnNullability =
        (TableChange.UpdateColumnNullability)
            TableChange.updateColumnNullability(new String[] {"col1"}, true);
    org.apache.gravitino.rel.TableChange gravitinoChange =
        sparkTableChangeConverter.toGravitinoTableChange(sparkUpdateColumnNullability);

    Assertions.assertTrue(
        gravitinoChange instanceof org.apache.gravitino.rel.TableChange.UpdateColumnNullability);
    org.apache.gravitino.rel.TableChange.UpdateColumnNullability gravitinoUpdateColumnNullability =
        (org.apache.gravitino.rel.TableChange.UpdateColumnNullability) gravitinoChange;

    Assertions.assertArrayEquals(
        sparkUpdateColumnNullability.fieldNames(), gravitinoUpdateColumnNullability.fieldName());
    Assertions.assertEquals(
        sparkUpdateColumnNullability.nullable(), gravitinoUpdateColumnNullability.nullable());
  }
}
