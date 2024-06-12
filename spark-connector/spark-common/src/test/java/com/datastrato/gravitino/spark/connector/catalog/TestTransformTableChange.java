/*
 *  Copyright 2024 Datastrato Pvt Ltd.
 *  This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.spark.connector.catalog;

import com.datastrato.gravitino.rel.TableChange.UpdateComment;
import com.datastrato.gravitino.spark.connector.ConnectorConstants;
import com.datastrato.gravitino.spark.connector.SparkTableChangeConverter;
import com.datastrato.gravitino.spark.connector.SparkTypeConverter;
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
    com.datastrato.gravitino.rel.TableChange tableChange =
        sparkTableChangeConverter.toGravitinoTableChange(sparkSetProperty);
    Assertions.assertTrue(
        tableChange instanceof com.datastrato.gravitino.rel.TableChange.SetProperty);
    com.datastrato.gravitino.rel.TableChange.SetProperty gravitinoSetProperty =
        (com.datastrato.gravitino.rel.TableChange.SetProperty) tableChange;
    Assertions.assertEquals("key", gravitinoSetProperty.getProperty());
    Assertions.assertEquals("value", gravitinoSetProperty.getValue());
  }

  @Test
  void testTransformRemoveProperty() {
    TableChange sparkRemoveProperty = TableChange.removeProperty("key");
    com.datastrato.gravitino.rel.TableChange tableChange =
        sparkTableChangeConverter.toGravitinoTableChange(sparkRemoveProperty);
    Assertions.assertTrue(
        tableChange instanceof com.datastrato.gravitino.rel.TableChange.RemoveProperty);
    com.datastrato.gravitino.rel.TableChange.RemoveProperty gravitinoRemoveProperty =
        (com.datastrato.gravitino.rel.TableChange.RemoveProperty) tableChange;
    Assertions.assertEquals("key", gravitinoRemoveProperty.getProperty());
  }

  @Test
  void testTransformUpdateComment() {
    TableChange sparkSetProperty = TableChange.setProperty(ConnectorConstants.COMMENT, "a");
    com.datastrato.gravitino.rel.TableChange tableChange =
        sparkTableChangeConverter.toGravitinoTableChange(sparkSetProperty);
    Assertions.assertTrue(
        tableChange instanceof com.datastrato.gravitino.rel.TableChange.UpdateComment);
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
    com.datastrato.gravitino.rel.TableChange gravitinoChange =
        sparkTableChangeConverter.toGravitinoTableChange(sparkRenameColumn);

    Assertions.assertTrue(
        gravitinoChange instanceof com.datastrato.gravitino.rel.TableChange.RenameColumn);
    com.datastrato.gravitino.rel.TableChange.RenameColumn gravitinoRenameColumn =
        (com.datastrato.gravitino.rel.TableChange.RenameColumn) gravitinoChange;

    Assertions.assertArrayEquals(oldFieldsName, gravitinoRenameColumn.getFieldName());
    Assertions.assertEquals(newFiledName, gravitinoRenameColumn.getNewName());
  }

  @Test
  void testTransformUpdateColumnComment() {
    String[] fieldNames = new String[] {"default_name"};
    String newComment = "default_comment";

    TableChange.UpdateColumnComment updateColumnComment =
        (TableChange.UpdateColumnComment) TableChange.updateColumnComment(fieldNames, newComment);
    com.datastrato.gravitino.rel.TableChange gravitinoChange =
        sparkTableChangeConverter.toGravitinoTableChange(updateColumnComment);

    Assertions.assertTrue(
        gravitinoChange instanceof com.datastrato.gravitino.rel.TableChange.UpdateColumnComment);
    com.datastrato.gravitino.rel.TableChange.UpdateColumnComment gravitinoUpdateColumnComment =
        (com.datastrato.gravitino.rel.TableChange.UpdateColumnComment) gravitinoChange;

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
    com.datastrato.gravitino.rel.TableChange gravitinoChangeFirst =
        sparkTableChangeConverter.toGravitinoTableChange(sparkAddColumnFirst);

    Assertions.assertTrue(
        gravitinoChangeFirst instanceof com.datastrato.gravitino.rel.TableChange.AddColumn);
    com.datastrato.gravitino.rel.TableChange.AddColumn gravitinoAddColumnFirst =
        (com.datastrato.gravitino.rel.TableChange.AddColumn) gravitinoChangeFirst;

    Assertions.assertArrayEquals(
        sparkAddColumnFirst.fieldNames(), gravitinoAddColumnFirst.fieldName());
    Assertions.assertTrue(
        "string".equalsIgnoreCase(gravitinoAddColumnFirst.getDataType().simpleString()));
    Assertions.assertTrue(
        gravitinoAddColumnFirst.getPosition()
            instanceof com.datastrato.gravitino.rel.TableChange.First);

    TableChange.AddColumn sparkAddColumnAfter =
        (TableChange.AddColumn)
            TableChange.addColumn(new String[] {"col1"}, DataTypes.StringType, true, "", after);
    com.datastrato.gravitino.rel.TableChange gravitinoChangeAfter =
        sparkTableChangeConverter.toGravitinoTableChange(sparkAddColumnAfter);

    Assertions.assertTrue(
        gravitinoChangeAfter instanceof com.datastrato.gravitino.rel.TableChange.AddColumn);
    com.datastrato.gravitino.rel.TableChange.AddColumn gravitinoAddColumnAfter =
        (com.datastrato.gravitino.rel.TableChange.AddColumn) gravitinoChangeAfter;

    Assertions.assertArrayEquals(
        sparkAddColumnAfter.fieldNames(), gravitinoAddColumnAfter.fieldName());
    Assertions.assertTrue(
        "string".equalsIgnoreCase(gravitinoAddColumnAfter.getDataType().simpleString()));
    Assertions.assertTrue(
        gravitinoAddColumnAfter.getPosition()
            instanceof com.datastrato.gravitino.rel.TableChange.After);

    TableChange.AddColumn sparkAddColumnDefault =
        (TableChange.AddColumn)
            TableChange.addColumn(new String[] {"col1"}, DataTypes.StringType, true, "", null);
    com.datastrato.gravitino.rel.TableChange gravitinoChangeDefault =
        sparkTableChangeConverter.toGravitinoTableChange(sparkAddColumnDefault);

    Assertions.assertTrue(
        gravitinoChangeDefault instanceof com.datastrato.gravitino.rel.TableChange.AddColumn);
    com.datastrato.gravitino.rel.TableChange.AddColumn gravitinoAddColumnDefault =
        (com.datastrato.gravitino.rel.TableChange.AddColumn) gravitinoChangeDefault;

    Assertions.assertArrayEquals(
        sparkAddColumnDefault.fieldNames(), gravitinoAddColumnDefault.fieldName());
    Assertions.assertTrue(
        "string".equalsIgnoreCase(gravitinoAddColumnDefault.getDataType().simpleString()));
    Assertions.assertTrue(
        gravitinoAddColumnDefault.getPosition()
            instanceof com.datastrato.gravitino.rel.TableChange.Default);
  }

  @Test
  void testTransformDeleteColumn() {
    TableChange.DeleteColumn sparkDeleteColumn =
        (TableChange.DeleteColumn) TableChange.deleteColumn(new String[] {"col1"}, true);
    com.datastrato.gravitino.rel.TableChange gravitinoChange =
        sparkTableChangeConverter.toGravitinoTableChange(sparkDeleteColumn);

    Assertions.assertTrue(
        gravitinoChange instanceof com.datastrato.gravitino.rel.TableChange.DeleteColumn);
    com.datastrato.gravitino.rel.TableChange.DeleteColumn gravitinoDeleteColumn =
        (com.datastrato.gravitino.rel.TableChange.DeleteColumn) gravitinoChange;

    Assertions.assertArrayEquals(sparkDeleteColumn.fieldNames(), gravitinoDeleteColumn.fieldName());
    Assertions.assertEquals(sparkDeleteColumn.ifExists(), gravitinoDeleteColumn.getIfExists());
  }

  @Test
  void testTransformUpdateColumnType() {
    TableChange.UpdateColumnType sparkUpdateColumnType =
        (TableChange.UpdateColumnType)
            TableChange.updateColumnType(new String[] {"col1"}, DataTypes.StringType);
    com.datastrato.gravitino.rel.TableChange gravitinoChange =
        sparkTableChangeConverter.toGravitinoTableChange(sparkUpdateColumnType);

    Assertions.assertTrue(
        gravitinoChange instanceof com.datastrato.gravitino.rel.TableChange.UpdateColumnType);
    com.datastrato.gravitino.rel.TableChange.UpdateColumnType gravitinoUpdateColumnType =
        (com.datastrato.gravitino.rel.TableChange.UpdateColumnType) gravitinoChange;

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
    com.datastrato.gravitino.rel.TableChange gravitinoChangeFirst =
        sparkTableChangeConverter.toGravitinoTableChange(sparkUpdateColumnFirst);

    Assertions.assertTrue(
        gravitinoChangeFirst
            instanceof com.datastrato.gravitino.rel.TableChange.UpdateColumnPosition);
    com.datastrato.gravitino.rel.TableChange.UpdateColumnPosition gravitinoUpdateColumnFirst =
        (com.datastrato.gravitino.rel.TableChange.UpdateColumnPosition) gravitinoChangeFirst;

    Assertions.assertArrayEquals(
        sparkUpdateColumnFirst.fieldNames(), gravitinoUpdateColumnFirst.fieldName());
    Assertions.assertTrue(
        gravitinoUpdateColumnFirst.getPosition()
            instanceof com.datastrato.gravitino.rel.TableChange.First);

    TableChange.UpdateColumnPosition sparkUpdateColumnAfter =
        (TableChange.UpdateColumnPosition)
            TableChange.updateColumnPosition(new String[] {"col1"}, after);
    com.datastrato.gravitino.rel.TableChange gravitinoChangeAfter =
        sparkTableChangeConverter.toGravitinoTableChange(sparkUpdateColumnAfter);

    Assertions.assertTrue(
        gravitinoChangeAfter
            instanceof com.datastrato.gravitino.rel.TableChange.UpdateColumnPosition);
    com.datastrato.gravitino.rel.TableChange.UpdateColumnPosition gravitinoUpdateColumnAfter =
        (com.datastrato.gravitino.rel.TableChange.UpdateColumnPosition) gravitinoChangeAfter;

    Assertions.assertArrayEquals(
        sparkUpdateColumnAfter.fieldNames(), gravitinoUpdateColumnAfter.fieldName());
    Assertions.assertTrue(
        gravitinoUpdateColumnAfter.getPosition()
            instanceof com.datastrato.gravitino.rel.TableChange.After);
  }

  @Test
  void testTransformUpdateColumnNullability() {
    TableChange.UpdateColumnNullability sparkUpdateColumnNullability =
        (TableChange.UpdateColumnNullability)
            TableChange.updateColumnNullability(new String[] {"col1"}, true);
    com.datastrato.gravitino.rel.TableChange gravitinoChange =
        sparkTableChangeConverter.toGravitinoTableChange(sparkUpdateColumnNullability);

    Assertions.assertTrue(
        gravitinoChange
            instanceof com.datastrato.gravitino.rel.TableChange.UpdateColumnNullability);
    com.datastrato.gravitino.rel.TableChange.UpdateColumnNullability
        gravitinoUpdateColumnNullability =
            (com.datastrato.gravitino.rel.TableChange.UpdateColumnNullability) gravitinoChange;

    Assertions.assertArrayEquals(
        sparkUpdateColumnNullability.fieldNames(), gravitinoUpdateColumnNullability.fieldName());
    Assertions.assertEquals(
        sparkUpdateColumnNullability.nullable(), gravitinoUpdateColumnNullability.nullable());
  }
}
