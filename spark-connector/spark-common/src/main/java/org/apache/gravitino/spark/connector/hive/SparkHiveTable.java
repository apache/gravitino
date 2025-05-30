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

package org.apache.gravitino.spark.connector.hive;

import com.google.common.base.Preconditions;
import java.util.Map;
import org.apache.gravitino.rel.Table;
import org.apache.gravitino.spark.connector.PropertiesConverter;
import org.apache.gravitino.spark.connector.SparkTransformConverter;
import org.apache.gravitino.spark.connector.SparkTypeConverter;
import org.apache.gravitino.spark.connector.utils.GravitinoTableInfoHelper;
import org.apache.gravitino.spark.connector.utils.HiveGravitinoOperationOperator;
import org.apache.kyuubi.spark.connector.hive.HiveTable;
import org.apache.kyuubi.spark.connector.hive.HiveTableCatalog;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.analysis.NoSuchPartitionException;
import org.apache.spark.sql.catalyst.analysis.PartitionAlreadyExistsException;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.SupportsPartitionManagement;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.types.StructType;

/** Keep consistent behavior with the SparkIcebergTable */
public class SparkHiveTable extends HiveTable implements SupportsPartitionManagement {

  private GravitinoTableInfoHelper gravitinoTableInfoHelper;
  private HiveGravitinoOperationOperator hiveGravitinoOperationOperator;

  public SparkHiveTable(
      Identifier identifier,
      Table gravitinoTable,
      HiveTable hiveTable,
      HiveTableCatalog hiveTableCatalog,
      PropertiesConverter propertiesConverter,
      SparkTransformConverter sparkTransformConverter,
      SparkTypeConverter sparkTypeConverter) {
    super(SparkSession.active(), hiveTable.catalogTable(), hiveTableCatalog);
    this.gravitinoTableInfoHelper =
        new GravitinoTableInfoHelper(
            false,
            identifier,
            gravitinoTable,
            propertiesConverter,
            sparkTransformConverter,
            sparkTypeConverter);
    this.hiveGravitinoOperationOperator = new HiveGravitinoOperationOperator(gravitinoTable);
  }

  @Override
  public String name() {
    return gravitinoTableInfoHelper.name();
  }

  @Override
  @SuppressWarnings("deprecation")
  public StructType schema() {
    return gravitinoTableInfoHelper.schema();
  }

  @Override
  public Map<String, String> properties() {
    return gravitinoTableInfoHelper.properties();
  }

  @Override
  public Transform[] partitioning() {
    return gravitinoTableInfoHelper.partitioning();
  }

  @Override
  public void createPartition(InternalRow ident, Map<String, String> properties)
      throws PartitionAlreadyExistsException, UnsupportedOperationException {
    hiveGravitinoOperationOperator.createPartition(ident, properties, partitionSchema());
  }

  @Override
  public boolean dropPartition(InternalRow ident) {
    return hiveGravitinoOperationOperator.dropPartition(ident, partitionSchema());
  }

  @Override
  public void replacePartitionMetadata(InternalRow ident, Map<String, String> properties)
      throws NoSuchPartitionException, UnsupportedOperationException {
    throw new UnsupportedOperationException("Replace partition is not supported");
  }

  @Override
  public Map<String, String> loadPartitionMetadata(InternalRow ident)
      throws UnsupportedOperationException {
    return hiveGravitinoOperationOperator.loadPartitionMetadata(ident, partitionSchema());
  }

  @Override
  public InternalRow[] listPartitionIdentifiers(String[] names, InternalRow ident) {
    return hiveGravitinoOperationOperator.listPartitionIdentifiers(names, ident, partitionSchema());
  }

  @Override
  public boolean partitionExists(InternalRow ident) {
    String[] partitionNames = partitionSchema().names();
    Preconditions.checkArgument(
        ident.numFields() == partitionNames.length,
        String.format(
            "The number of fields (%d) in the partition identifier is not equal to "
                + "the partition schema length (%d). "
                + "The identifier might not refer to one partition.",
            ident.numFields(), partitionNames.length));

    return hiveGravitinoOperationOperator.partitionExists(partitionNames, ident, partitionSchema());
  }

  @Override
  public Object productElement(int n) {
    if (n == 0) {
      return gravitinoTableInfoHelper;
    }

    if (n == 1) {
      return hiveGravitinoOperationOperator;
    }

    throw new IndexOutOfBoundsException("Invalid index: " + n);
  }

  @Override
  public int productArity() {
    return 2;
  }

  @Override
  public boolean canEqual(Object that) {
    return that instanceof SparkHiveTable;
  }
}
