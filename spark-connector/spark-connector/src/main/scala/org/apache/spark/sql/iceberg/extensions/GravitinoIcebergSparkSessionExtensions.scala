/*
 *  Copyright 2024 Datastrato Pvt Ltd.
 *  This software is licensed under the Apache License version 2.
 */

package org.apache.spark.sql.iceberg.extensions

import org.apache.spark.sql.SparkSessionExtensions
import org.apache.spark.sql.iceberg.catalyst.parser.GravitinoIcebergSparkSqlExtensionsParser

class GravitinoIcebergSparkSessionExtensions extends (SparkSessionExtensions => Unit) {

  override def apply(extensions: SparkSessionExtensions): Unit = {
      // parser extensions
      extensions.injectParser { case (_, parser) => new GravitinoIcebergSparkSqlExtensionsParser(parser) }
  }
}
