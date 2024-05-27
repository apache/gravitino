/*
 *  Copyright 2024 Datastrato Pvt Ltd.
 *  This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.spark.connector;

import com.datastrato.gravitino.rel.expressions.literals.Literals;
import org.apache.spark.sql.connector.catalog.TableChange;

public class SparkTableChangeConverter34 extends SparkTableChangeConverter {
  public SparkTableChangeConverter34(SparkTypeConverter sparkTypeConverter) {
    super(sparkTypeConverter);
  }

  public com.datastrato.gravitino.rel.TableChange toGravitinoTableChange(TableChange change) {
    if (change instanceof TableChange.UpdateColumnDefaultValue) {
      TableChange.UpdateColumnDefaultValue updateColumnDefaultValue =
          (TableChange.UpdateColumnDefaultValue) change;
      return com.datastrato.gravitino.rel.TableChange.updateColumnDefaultValue(
          updateColumnDefaultValue.fieldNames(),
          Literals.stringLiteral(updateColumnDefaultValue.newDefaultValue()));
    } else {
      return super.toGravitinoTableChange(change);
    }
  }
}
