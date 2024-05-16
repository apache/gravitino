/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.spark.connector.iceberg.extensions;

import com.datastrato.gravitino.spark.connector.iceberg.GravitinoIcebergCatalog;
import java.util.Collections;
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
import scala.collection.Seq;

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
      return IcebergCatalogAndIdentifier.buildCatalogAndIdentifier(spark, addPartitionField.table())
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
          .getOrElse(() -> super.apply(plan));
    } else if (plan instanceof CreateOrReplaceBranch) {
      CreateOrReplaceBranch createOrReplaceBranch = (CreateOrReplaceBranch) plan;
      return IcebergCatalogAndIdentifier.buildCatalogAndIdentifier(
              spark, createOrReplaceBranch.table())
          .map(
              catalogAndIdentifier -> {
                CreateOrReplaceBranchExec createOrReplaceBranchExec =
                    new CreateOrReplaceBranchExec(
                        catalogAndIdentifier.catalog,
                        catalogAndIdentifier.identifier,
                        createOrReplaceBranch.branch(),
                        createOrReplaceBranch.branchOptions(),
                        createOrReplaceBranch.replace(),
                        createOrReplaceBranch.ifNotExists());
                return toSeq(createOrReplaceBranchExec);
              })
          .getOrElse(() -> super.apply(plan));
    } else if (plan instanceof CreateOrReplaceTag) {
      CreateOrReplaceTag createOrReplaceTag = (CreateOrReplaceTag) plan;
      return IcebergCatalogAndIdentifier.buildCatalogAndIdentifier(
              spark, createOrReplaceTag.table())
          .map(
              catalogAndIdentifier -> {
                CreateOrReplaceTagExec createOrReplaceTagExec =
                    new CreateOrReplaceTagExec(
                        catalogAndIdentifier.catalog,
                        catalogAndIdentifier.identifier,
                        createOrReplaceTag.tag(),
                        createOrReplaceTag.tagOptions(),
                        createOrReplaceTag.replace(),
                        createOrReplaceTag.ifNotExists());
                return toSeq(createOrReplaceTagExec);
              })
          .getOrElse(() -> super.apply(plan));
    } else if (plan instanceof DropBranch) {
      DropBranch dropBranch = (DropBranch) plan;
      return IcebergCatalogAndIdentifier.buildCatalogAndIdentifier(spark, dropBranch.table())
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
          .getOrElse(() -> super.apply(plan));
    } else if (plan instanceof DropTag) {
      DropTag dropTag = (DropTag) plan;
      return IcebergCatalogAndIdentifier.buildCatalogAndIdentifier(spark, dropTag.table())
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
          .getOrElse(() -> super.apply(plan));
    } else if (plan instanceof DropPartitionField) {
      DropPartitionField dropPartitionField = (DropPartitionField) plan;
      return IcebergCatalogAndIdentifier.buildCatalogAndIdentifier(
              spark, dropPartitionField.table())
          .map(
              catalogAndIdentifier -> {
                DropPartitionFieldExec dropPartitionFieldExec =
                    new DropPartitionFieldExec(
                        catalogAndIdentifier.catalog,
                        catalogAndIdentifier.identifier,
                        dropPartitionField.transform());
                return toSeq(dropPartitionFieldExec);
              })
          .getOrElse(() -> super.apply(plan));
    } else if (plan instanceof ReplacePartitionField) {
      ReplacePartitionField replacePartitionField = (ReplacePartitionField) plan;
      return IcebergCatalogAndIdentifier.buildCatalogAndIdentifier(
              spark, replacePartitionField.table())
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
          .getOrElse(() -> super.apply(plan));
    } else if (plan instanceof SetIdentifierFields) {
      SetIdentifierFields setIdentifierFields = (SetIdentifierFields) plan;
      return IcebergCatalogAndIdentifier.buildCatalogAndIdentifier(
              spark, setIdentifierFields.table())
          .map(
              catalogAndIdentifier -> {
                SetIdentifierFieldsExec setIdentifierFieldsExec =
                    new SetIdentifierFieldsExec(
                        catalogAndIdentifier.catalog,
                        catalogAndIdentifier.identifier,
                        setIdentifierFields.fields());
                return toSeq(setIdentifierFieldsExec);
              })
          .getOrElse(() -> super.apply(plan));
    } else if (plan instanceof DropIdentifierFields) {
      DropIdentifierFields dropIdentifierFields = (DropIdentifierFields) plan;
      return IcebergCatalogAndIdentifier.buildCatalogAndIdentifier(
              spark, dropIdentifierFields.table())
          .map(
              catalogAndIdentifier -> {
                DropIdentifierFieldsExec dropIdentifierFieldsExec =
                    new DropIdentifierFieldsExec(
                        catalogAndIdentifier.catalog,
                        catalogAndIdentifier.identifier,
                        dropIdentifierFields.fields());
                return toSeq(dropIdentifierFieldsExec);
              })
          .getOrElse(() -> super.apply(plan));
    } else if (plan instanceof SetWriteDistributionAndOrdering) {
      SetWriteDistributionAndOrdering setWriteDistributionAndOrdering =
          (SetWriteDistributionAndOrdering) plan;
      return IcebergCatalogAndIdentifier.buildCatalogAndIdentifier(
              spark, setWriteDistributionAndOrdering.table())
          .map(
              catalogAndIdentifier -> {
                SetWriteDistributionAndOrderingExec setWriteDistributionAndOrderingExec =
                    new SetWriteDistributionAndOrderingExec(
                        catalogAndIdentifier.catalog,
                        catalogAndIdentifier.identifier,
                        setWriteDistributionAndOrdering.distributionMode(),
                        setWriteDistributionAndOrdering.sortOrder());
                return toSeq(setWriteDistributionAndOrderingExec);
              })
          .getOrElse(() -> super.apply(plan));
    } else {
      return super.apply(plan);
    }
  }

  private Seq<SparkPlan> toSeq(SparkPlan plan) {
    return JavaConverters.asScalaIteratorConverter(Collections.singletonList(plan).listIterator())
        .asScala()
        .toSeq();
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
          Spark3Util.catalogAndIdentifier(spark, JavaConverters.<String>seqAsJavaList(identifiers));
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
}
