/*
 *  Copyright 2024 Datastrato Pvt Ltd.
 *  This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.spark.connector.catalog;

import org.apache.spark.sql.connector.catalog.ColumnDefaultValue;
import org.apache.spark.sql.connector.catalog.TableChange;
import org.apache.spark.sql.connector.expressions.LiteralValue;
import org.apache.spark.sql.types.DataTypes;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestTransformTableChange {

  @Test
  void testTransformSetProperty() {
    TableChange sparkSetProperty = TableChange.setProperty("key", "value");
    com.datastrato.gravitino.rel.TableChange tableChange =
        GravitinoCatalog.transformTableChange(sparkSetProperty);
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
        GravitinoCatalog.transformTableChange(sparkRemoveProperty);
    Assertions.assertTrue(
        tableChange instanceof com.datastrato.gravitino.rel.TableChange.RemoveProperty);
    com.datastrato.gravitino.rel.TableChange.RemoveProperty gravitinoRemoveProperty =
        (com.datastrato.gravitino.rel.TableChange.RemoveProperty) tableChange;
    Assertions.assertEquals("key", gravitinoRemoveProperty.getProperty());
  }

  @Test
  void testTransformAddColumn() {

    TableChange.ColumnPosition first = TableChange.ColumnPosition.first();
    TableChange.ColumnPosition after = TableChange.ColumnPosition.after("col0");
    ColumnDefaultValue defaultValue =
        new ColumnDefaultValue(
            "CURRENT_DEFAULT", new LiteralValue("default_value", DataTypes.StringType));

    TableChange.AddColumn sparkAddColumnFirst =
        (TableChange.AddColumn)
            TableChange.addColumn(
                new String[] {"col1"}, DataTypes.StringType, true, "", first, defaultValue);
    com.datastrato.gravitino.rel.TableChange gravitinoChangeFirst =
        GravitinoCatalog.transformTableChange(sparkAddColumnFirst);

    Assertions.assertTrue(
        gravitinoChangeFirst instanceof com.datastrato.gravitino.rel.TableChange.AddColumn);
    com.datastrato.gravitino.rel.TableChange.AddColumn gravitinoAddColumnFirst =
        (com.datastrato.gravitino.rel.TableChange.AddColumn) gravitinoChangeFirst;

    Assertions.assertEquals(sparkAddColumnFirst.fieldNames(), gravitinoAddColumnFirst.fieldName());
    Assertions.assertTrue(
        "string".equalsIgnoreCase(gravitinoAddColumnFirst.getDataType().simpleString()));
    Assertions.assertTrue(
        gravitinoAddColumnFirst.getPosition()
            instanceof com.datastrato.gravitino.rel.TableChange.First);

    TableChange.AddColumn sparkAddColumnAfter =
        (TableChange.AddColumn)
            TableChange.addColumn(
                new String[] {"col1"}, DataTypes.StringType, true, "", after, defaultValue);
    com.datastrato.gravitino.rel.TableChange gravitinoChangeAfter =
        GravitinoCatalog.transformTableChange(sparkAddColumnAfter);

    Assertions.assertTrue(
        gravitinoChangeAfter instanceof com.datastrato.gravitino.rel.TableChange.AddColumn);
    com.datastrato.gravitino.rel.TableChange.AddColumn gravitinoAddColumnAfter =
        (com.datastrato.gravitino.rel.TableChange.AddColumn) gravitinoChangeAfter;

    Assertions.assertEquals(sparkAddColumnAfter.fieldNames(), gravitinoAddColumnAfter.fieldName());
    Assertions.assertTrue(
        "string".equalsIgnoreCase(gravitinoAddColumnAfter.getDataType().simpleString()));
    Assertions.assertTrue(
        gravitinoAddColumnAfter.getPosition()
            instanceof com.datastrato.gravitino.rel.TableChange.After);

    TableChange.AddColumn sparkAddColumnDefault =
        (TableChange.AddColumn)
            TableChange.addColumn(
                new String[] {"col1"}, DataTypes.StringType, true, "", null, defaultValue);
    com.datastrato.gravitino.rel.TableChange gravitinoChangeDefault =
        GravitinoCatalog.transformTableChange(sparkAddColumnDefault);

    Assertions.assertTrue(
        gravitinoChangeDefault instanceof com.datastrato.gravitino.rel.TableChange.AddColumn);
    com.datastrato.gravitino.rel.TableChange.AddColumn gravitinoAddColumnDefault =
        (com.datastrato.gravitino.rel.TableChange.AddColumn) gravitinoChangeDefault;

    Assertions.assertEquals(
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
        GravitinoCatalog.transformTableChange(sparkDeleteColumn);

    Assertions.assertTrue(
        gravitinoChange instanceof com.datastrato.gravitino.rel.TableChange.DeleteColumn);
    com.datastrato.gravitino.rel.TableChange.DeleteColumn gravitinoDeleteColumn =
        (com.datastrato.gravitino.rel.TableChange.DeleteColumn) gravitinoChange;

    Assertions.assertEquals(sparkDeleteColumn.fieldNames(), gravitinoDeleteColumn.fieldName());
    Assertions.assertEquals(sparkDeleteColumn.ifExists(), gravitinoDeleteColumn.getIfExists());
  }
}
