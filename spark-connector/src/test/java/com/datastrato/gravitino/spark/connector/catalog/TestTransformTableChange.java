/*
 *  Copyright 2024 Datastrato Pvt Ltd.
 *  This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.spark.connector.catalog;

import org.apache.spark.sql.connector.catalog.TableChange;
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
    TableChange.AddColumn sparkAddColumn =
        (TableChange.AddColumn) TableChange.addColumn(new String[] {"col1"}, DataTypes.StringType);
    com.datastrato.gravitino.rel.TableChange gravitinoChange =
        GravitinoCatalog.transformTableChange(sparkAddColumn);

    Assertions.assertTrue(
        gravitinoChange instanceof com.datastrato.gravitino.rel.TableChange.AddColumn);
    com.datastrato.gravitino.rel.TableChange.AddColumn gravitinoAddColumn =
        (com.datastrato.gravitino.rel.TableChange.AddColumn) gravitinoChange;

    Assertions.assertEquals(sparkAddColumn.fieldNames(), gravitinoAddColumn.fieldName());
    Assertions.assertTrue(
        "string".equalsIgnoreCase(gravitinoAddColumn.getDataType().simpleString()));
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
