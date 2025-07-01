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
package org.apache.gravitino.spark.connector.iceberg.extensions;

import static org.apache.gravitino.spark.connector.utils.ConnectorUtil.toJavaList;

import java.lang.reflect.Method;
import java.util.Collections;
import lombok.SneakyThrows;
import org.apache.gravitino.spark.connector.iceberg.GravitinoIcebergCatalog;
import org.apache.iceberg.spark.Spark3Util;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.plans.logical.AddPartitionField;
import org.apache.spark.sql.catalyst.plans.logical.CreateOrReplaceBranch;
import org.apache.spark.sql.catalyst.plans.logical.CreateOrReplaceTag;
import org.apache.spark.sql.catalyst.plans.logical.DropBranch;
import org.apache.spark.sql.catalyst.plans.logical.DropIdentifierFields;
import org.apache.spark.sql.catalyst.plans.logical.DropPartitionField;
import org.apache.spark.sql.catalyst.plans.logical.DropTag;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.catalyst.plans.logical.ReplacePartitionField;
import org.apache.spark.sql.catalyst.plans.logical.SetIdentifierFields;
import org.apache.spark.sql.catalyst.plans.logical.SetWriteDistributionAndOrdering;
import org.apache.spark.sql.connector.catalog.CatalogPlugin;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.execution.SparkPlan;
import org.apache.spark.sql.execution.datasources.v2.AddPartitionFieldExec;
import org.apache.spark.sql.execution.datasources.v2.CreateOrReplaceBranchExec;
import org.apache.spark.sql.execution.datasources.v2.CreateOrReplaceTagExec;
import org.apache.spark.sql.execution.datasources.v2.DropBranchExec;
import org.apache.spark.sql.execution.datasources.v2.DropIdentifierFieldsExec;
import org.apache.spark.sql.execution.datasources.v2.DropPartitionFieldExec;
import org.apache.spark.sql.execution.datasources.v2.DropTagExec;
import org.apache.spark.sql.execution.datasources.v2.ExtendedDataSourceV2Strategy;
import org.apache.spark.sql.execution.datasources.v2.ReplacePartitionFieldExec;
import org.apache.spark.sql.execution.datasources.v2.SetIdentifierFieldsExec;
import org.apache.spark.sql.execution.datasources.v2.SetWriteDistributionAndOrderingExec;
import scala.Option;
import scala.Some;
import scala.collection.JavaConverters;
import scala.collection.immutable.Seq;

public class IcebergExtendedDataSourceV2Strategy extends ExtendedDataSourceV2Strategy {

  private final SparkSession spark;

  public IcebergExtendedDataSourceV2Strategy(SparkSession spark) {
    super(spark);
    this.spark = spark;
  }

  @Override
  public Seq<SparkPlan> apply(LogicalPlan plan) {
    if (plan instanceof AddPartitionField) {
      AddPartitionField addPartitionField = (AddPartitionField) plan;
      return IcebergCatalogAndIdentifier.buildCatalogAndIdentifier(
              spark, addPartitionField.table().toIndexedSeq())
          .map(
              catalogAndIdentifier -> {
                AddPartitionFieldExec addPartitionFieldExec =
                    new AddPartitionFieldExec(
                        catalogAndIdentifier.catalog,
                        catalogAndIdentifier.identifier,
                        addPartitionField.transform(),
                        addPartitionField.name());
                return toSeq(addPartitionFieldExec);
              })
          .get();
    } else if (plan instanceof CreateOrReplaceBranch) {
      CreateOrReplaceBranch createOrReplaceBranch = (CreateOrReplaceBranch) plan;
      return IcebergCatalogAndIdentifier.buildCatalogAndIdentifier(
              spark, createOrReplaceBranch.table().toIndexedSeq())
          .map(
              catalogAndIdentifier -> {
                CreateOrReplaceBranchExec createOrReplaceBranchExec =
                    new CreateOrReplaceBranchExec(
                        catalogAndIdentifier.catalog,
                        catalogAndIdentifier.identifier,
                        createOrReplaceBranch.branch(),
                        createOrReplaceBranch.branchOptions(),
                        createOrReplaceBranch.create(),
                        createOrReplaceBranch.replace(),
                        createOrReplaceBranch.ifNotExists());
                return toSeq(createOrReplaceBranchExec);
              })
          .get();
    } else if (plan instanceof CreateOrReplaceTag) {
      CreateOrReplaceTag createOrReplaceTag = (CreateOrReplaceTag) plan;
      return IcebergCatalogAndIdentifier.buildCatalogAndIdentifier(
              spark, createOrReplaceTag.table().toIndexedSeq())
          .map(
              catalogAndIdentifier -> {
                CreateOrReplaceTagExec createOrReplaceTagExec =
                    new CreateOrReplaceTagExec(
                        catalogAndIdentifier.catalog,
                        catalogAndIdentifier.identifier,
                        createOrReplaceTag.tag(),
                        createOrReplaceTag.tagOptions(),
                        createOrReplaceTag.create(),
                        createOrReplaceTag.replace(),
                        createOrReplaceTag.ifNotExists());
                return toSeq(createOrReplaceTagExec);
              })
          .get();
    } else if (plan instanceof DropBranch) {
      DropBranch dropBranch = (DropBranch) plan;
      return IcebergCatalogAndIdentifier.buildCatalogAndIdentifier(
              spark, dropBranch.table().toIndexedSeq())
          .map(
              catalogAndIdentifier -> {
                DropBranchExec dropBranchExec =
                    new DropBranchExec(
                        catalogAndIdentifier.catalog,
                        catalogAndIdentifier.identifier,
                        dropBranch.branch(),
                        dropBranch.ifExists());
                return toSeq(dropBranchExec);
              })
          .get();
    } else if (plan instanceof DropTag) {
      DropTag dropTag = (DropTag) plan;
      return IcebergCatalogAndIdentifier.buildCatalogAndIdentifier(
              spark, dropTag.table().toIndexedSeq())
          .map(
              catalogAndIdentifier -> {
                DropTagExec dropTagExec =
                    new DropTagExec(
                        catalogAndIdentifier.catalog,
                        catalogAndIdentifier.identifier,
                        dropTag.tag(),
                        dropTag.ifExists());
                return toSeq(dropTagExec);
              })
          .get();
    } else if (plan instanceof DropPartitionField) {
      DropPartitionField dropPartitionField = (DropPartitionField) plan;
      return IcebergCatalogAndIdentifier.buildCatalogAndIdentifier(
              spark, dropPartitionField.table().toIndexedSeq())
          .map(
              catalogAndIdentifier -> {
                DropPartitionFieldExec dropPartitionFieldExec =
                    new DropPartitionFieldExec(
                        catalogAndIdentifier.catalog,
                        catalogAndIdentifier.identifier,
                        dropPartitionField.transform());
                return toSeq(dropPartitionFieldExec);
              })
          .get();
    } else if (plan instanceof ReplacePartitionField) {
      ReplacePartitionField replacePartitionField = (ReplacePartitionField) plan;
      return IcebergCatalogAndIdentifier.buildCatalogAndIdentifier(
              spark, replacePartitionField.table().toIndexedSeq())
          .map(
              catalogAndIdentifier -> {
                ReplacePartitionFieldExec replacePartitionFieldExec =
                    new ReplacePartitionFieldExec(
                        catalogAndIdentifier.catalog,
                        catalogAndIdentifier.identifier,
                        replacePartitionField.transformFrom(),
                        replacePartitionField.transformTo(),
                        replacePartitionField.name());
                return toSeq(replacePartitionFieldExec);
              })
          .get();
    } else if (plan instanceof SetIdentifierFields) {
      SetIdentifierFields setIdentifierFields = (SetIdentifierFields) plan;
      return IcebergCatalogAndIdentifier.buildCatalogAndIdentifier(
              spark, setIdentifierFields.table().toIndexedSeq())
          .map(
              catalogAndIdentifier -> {
                SetIdentifierFieldsExec setIdentifierFieldsExec =
                    new SetIdentifierFieldsExec(
                        catalogAndIdentifier.catalog,
                        catalogAndIdentifier.identifier,
                        setIdentifierFields.fields());
                return toSeq(setIdentifierFieldsExec);
              })
          .get();
    } else if (plan instanceof DropIdentifierFields) {
      DropIdentifierFields dropIdentifierFields = (DropIdentifierFields) plan;
      return IcebergCatalogAndIdentifier.buildCatalogAndIdentifier(
              spark, dropIdentifierFields.table().toIndexedSeq())
          .map(
              catalogAndIdentifier -> {
                DropIdentifierFieldsExec dropIdentifierFieldsExec =
                    new DropIdentifierFieldsExec(
                        catalogAndIdentifier.catalog,
                        catalogAndIdentifier.identifier,
                        dropIdentifierFields.fields());
                return toSeq(dropIdentifierFieldsExec);
              })
          .get();
    } else if (plan instanceof SetWriteDistributionAndOrdering) {
      SetWriteDistributionAndOrdering setWriteDistributionAndOrdering =
          (SetWriteDistributionAndOrdering) plan;
      return IcebergCatalogAndIdentifier.buildCatalogAndIdentifier(
              spark, setWriteDistributionAndOrdering.table().toIndexedSeq())
          .map(
              catalogAndIdentifier -> {
                SetWriteDistributionAndOrderingExec setWriteDistributionAndOrderingExec = null;
                // Iceberg 1.6 return DistributionMode, while Iceberg 1.9 return
                // Option<DistributionMode>
                Object distributionMode = getDistributionMode(setWriteDistributionAndOrdering);
                try {
                  setWriteDistributionAndOrderingExec =
                      createDistributionAndOrderingExec(
                          catalogAndIdentifier.catalog,
                          catalogAndIdentifier.identifier,
                          distributionMode,
                          setWriteDistributionAndOrdering.sortOrder());
                } catch (Exception e) {
                  throw new RuntimeException(e);
                }
                return toSeq(setWriteDistributionAndOrderingExec);
              })
          .get();
    } else {
      scala.collection.Seq<SparkPlan> sparkPlans = super.apply(plan);
      if (sparkPlans != null) {
        return sparkPlans.toIndexedSeq();
      }
      return null;
    }
  }

  private Seq<SparkPlan> toSeq(SparkPlan plan) {
    return JavaConverters.asScalaIteratorConverter(Collections.singletonList(plan).listIterator())
        .asScala()
        .toIndexedSeq();
  }

  static class IcebergCatalogAndIdentifier {

    private final TableCatalog catalog;
    private final Identifier identifier;

    private IcebergCatalogAndIdentifier(TableCatalog catalog, Identifier identifier) {
      this.catalog = catalog;
      this.identifier = identifier;
    }

    private static IcebergCatalogAndIdentifier of(TableCatalog catalog, Identifier identifier) {
      return new IcebergCatalogAndIdentifier(catalog, identifier);
    }

    static Option<IcebergCatalogAndIdentifier> buildCatalogAndIdentifier(
        SparkSession spark, Seq<String> identifiers) {
      Spark3Util.CatalogAndIdentifier catalogAndIdentifier =
          Spark3Util.catalogAndIdentifier(spark, toJavaList(identifiers));
      CatalogPlugin catalog = catalogAndIdentifier.catalog();
      if (catalog instanceof GravitinoIcebergCatalog) {
        return new Some<>(
            IcebergCatalogAndIdentifier.of(
                (TableCatalog) catalog, catalogAndIdentifier.identifier()));
      } else {
        // TODO: support SparkSessionCatalog
        throw new UnsupportedOperationException(
            "Unsupported catalog type: " + catalog.getClass().getName());
      }
    }
  }

  private static SetWriteDistributionAndOrderingExec createDistributionAndOrderingExec(
      TableCatalog catalog, Identifier identifier, Object distributionMode, Object sortOrder)
      throws Exception {
    // 1. get the object associated with the scala case class
    Class<?> companionClass =
        Class.forName(SetWriteDistributionAndOrderingExec.class.getName() + "$");
    Object companionInstance = companionClass.getField("MODULE$").get(null);

    // 2. get apply method
    Class<?>[] paramTypes = getApplyMethodParamTypes(companionClass);
    Method applyMethod = companionClass.getMethod("apply", paramTypes);

    // 3. invoke apply to create object
    return (SetWriteDistributionAndOrderingExec)
        applyMethod.invoke(companionInstance, catalog, identifier, distributionMode, sortOrder);
  }

  private static Class<?>[] getApplyMethodParamTypes(Class<?> companionClass) {
    // Scala may generate multiple apply method，use the apply method with 4 arguments
    for (Method method : companionClass.getMethods()) {
      if ("apply".equals(method.getName()) && method.getParameterCount() == 4) {
        return method.getParameterTypes();
      }
    }
    throw new IllegalStateException("Could find apply method with 4 arguments");
  }

  @SneakyThrows
  private static Object getDistributionMode(
      SetWriteDistributionAndOrdering setWriteDistributionAndOrdering) {
    Method distributionModeMethod =
        setWriteDistributionAndOrdering.getClass().getMethod("distributionMode");
    return distributionModeMethod.invoke(setWriteDistributionAndOrdering);
  }
}
