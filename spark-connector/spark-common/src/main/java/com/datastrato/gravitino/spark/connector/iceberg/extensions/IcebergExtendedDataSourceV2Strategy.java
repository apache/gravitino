/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.spark.connector.iceberg.extensions;

import com.datastrato.gravitino.spark.connector.iceberg.GravitinoIcebergCatalog;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.iceberg.spark.Spark3Util;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.connector.catalog.CatalogPlugin;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.execution.SparkPlan;
import org.apache.spark.sql.execution.datasources.v2.ExtendedDataSourceV2Strategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Option;
import scala.Some;
import scala.collection.JavaConverters;
import scala.collection.Seq;

public class IcebergExtendedDataSourceV2Strategy extends ExtendedDataSourceV2Strategy {

  private static final Logger LOG =
      LoggerFactory.getLogger(IcebergExtendedDataSourceV2Strategy.class);

  private final Map<String, String> icebergCommands =
      ImmutableMap.of(
          "org.apache.spark.sql.catalyst.plans.logical.AddPartitionField",
          "org.apache.spark.sql.execution.datasources.v2.AddPartitionFieldExec",
          "org.apache.spark.sql.catalyst.plans.logical.CreateOrReplaceBranch",
          "org.apache.spark.sql.execution.datasources.v2.CreateOrReplaceBranchExec",
          "org.apache.spark.sql.catalyst.plans.logical.CreateOrReplaceTag",
          "org.apache.spark.sql.execution.datasources.v2.CreateOrReplaceTagExec",
          "org.apache.spark.sql.catalyst.plans.logical.DropBranch",
          "org.apache.spark.sql.execution.datasources.v2.DropBranchExec",
          "org.apache.spark.sql.catalyst.plans.logical.DropIdentifierFields",
          "org.apache.spark.sql.execution.datasources.v2.DropIdentifierFieldsExec",
          "org.apache.spark.sql.catalyst.plans.logical.DropPartitionField",
          "org.apache.spark.sql.execution.datasources.v2.DropPartitionFieldExec",
          "org.apache.spark.sql.catalyst.plans.logical.DropTag",
          "org.apache.spark.sql.execution.datasources.v2.DropTagExec",
          "org.apache.spark.sql.catalyst.plans.logical.ReplacePartitionField",
          "org.apache.spark.sql.execution.datasources.v2.ReplacePartitionFieldExec",
          "org.apache.spark.sql.catalyst.plans.logical.SetIdentifierFields",
          "org.apache.spark.sql.execution.datasources.v2.SetIdentifierFieldsExec",
          "org.apache.spark.sql.catalyst.plans.logical.SetWriteDistributionAndOrdering",
          "org.apache.spark.sql.execution.datasources.v2.SetWriteDistributionAndOrderingExec");

  private final SparkSession spark;

  public IcebergExtendedDataSourceV2Strategy(SparkSession spark) {
    super(spark);
    this.spark = spark;
  }

  @Override
  public Seq<SparkPlan> apply(LogicalPlan plan) {
    if (isIcebergCommand(plan)) {
      Set<String> errors = new HashSet<>();
      List<Object> parameterValues = getLogicalPlanConstructorParams(plan, errors);

      if (!errors.isEmpty()) {
        throw new RuntimeException(
            String.format("Reflecting LogicalPlan: %s failed.", plan.getClass().getName()));
      }

      Seq<String> tableName = (Seq<String>) parameterValues.get(0);
      Option<Seq<SparkPlan>> physicalPlan =
          constructPhysicalPlan(plan, tableName, parameterValues, errors);

      if (errors.isEmpty()) {
        return physicalPlan.get();
      } else {
        throw new RuntimeException(
            String.format(
                "Constructing PhysicalPlan: %s failed.",
                icebergCommands.get(plan.getClass().getName())));
      }
    } else {
      return super.apply(plan);
    }
  }

  private boolean isIcebergCommand(LogicalPlan plan) {
    return icebergCommands.keySet().stream()
        .anyMatch(command -> command.equals(plan.getClass().getName()));
  }

  private List<Object> getLogicalPlanConstructorParams(LogicalPlan plan, Set<String> errors) {
    Class<? extends LogicalPlan> logicalPlanClazz = plan.getClass();
    String logicalPlanClassName = logicalPlanClazz.getName();
    Constructor<?>[] logicalPlanDeclaredConstructors = plan.getClass().getDeclaredConstructors();
    Preconditions.checkArgument(
        logicalPlanDeclaredConstructors.length == 1
            && logicalPlanDeclaredConstructors[0].getParameters().length > 0,
        String.format(
            "Scala case class: %s only have a constructor with parameters.", logicalPlanClassName));
    return Arrays.stream(logicalPlanDeclaredConstructors[0].getParameters())
        .map(
            parameter -> {
              try {
                Field field = logicalPlanClazz.getDeclaredField(parameter.getName());
                field.setAccessible(true);
                return field.get(plan);
              } catch (NoSuchFieldException | IllegalAccessException e) {
                LOG.error(
                    String.format(
                        "Failed to get value of field: %s from LogicalPlan: %s.",
                        parameter.getName(), logicalPlanClassName),
                    e);
                errors.add(e.getMessage());
                return null;
              }
            })
        .collect(Collectors.toList());
  }

  private Option<Seq<SparkPlan>> constructPhysicalPlan(
      LogicalPlan plan, Seq<String> tableName, List<Object> parameterValues, Set<String> errors) {
    return (Option<Seq<SparkPlan>>)
        IcebergCatalogAndIdentifier.buildCatalogAndIdentifier(spark, tableName)
            .map(
                catalogAndIdentifier -> {
                  String physicalPlanClassName = icebergCommands.get(plan.getClass().getName());
                  try {
                    Class<?> physicalPlanClazz = Class.forName(physicalPlanClassName);
                    Constructor<?>[] physicalPlanDeclaredConstructors =
                        physicalPlanClazz.getDeclaredConstructors();
                    Preconditions.checkArgument(
                        physicalPlanDeclaredConstructors.length == 1
                            && physicalPlanDeclaredConstructors[0].getParameters().length > 0,
                        String.format(
                            "Scala case class: %s only have a constructor with parameters.",
                            physicalPlanClassName));
                    SparkPlan sparkPlan =
                        (SparkPlan)
                            (physicalPlanDeclaredConstructors[0].newInstance(
                                catalogAndIdentifier.catalog,
                                catalogAndIdentifier.identifier,
                                parameterValues.remove(0)));
                    return toSeq(sparkPlan);
                  } catch (ClassNotFoundException
                      | InvocationTargetException
                      | InstantiationException
                      | IllegalAccessException e) {
                    LOG.error(
                        String.format(
                            "Failed to create a physicalPlan object: %s", physicalPlanClassName),
                        e);
                    errors.add(e.getMessage());
                    return null;
                  }
                });
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
