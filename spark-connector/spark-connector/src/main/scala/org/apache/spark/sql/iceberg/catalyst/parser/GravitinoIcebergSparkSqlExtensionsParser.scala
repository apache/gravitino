/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.spark.sql.iceberg.catalyst.parser

import scala.collection.JavaConverters._
import java.util.Locale
import scala.util.Try

import com.datastrato.gravitino.spark.connector.iceberg.SparkIcebergTable
import org.apache.iceberg.common.DynConstructors
import org.apache.iceberg.spark.Spark3Util

import org.apache.spark.sql.{AnalysisException, SparkSession}
import org.apache.spark.sql.catalyst.analysis.{EliminateSubqueryAliases, UnresolvedRelation}
import org.apache.spark.sql.catalyst.parser.ParserInterface
import org.apache.spark.sql.catalyst.parser.extensions.{IcebergSparkSqlExtensionsParser, IcebergSqlExtensionsAstBuilder}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.connector.catalog.{Table, TableCatalog}
import org.apache.spark.sql.execution.command.ExplainCommand
import org.apache.spark.sql.internal.{SQLConf, VariableSubstitution}

/**
 * When parsing RowLevelCommands, IcebergSparkSqlExtensionsParser only supports the Iceberg SparkTable
 * instead of the SparkIcebergTable defined by the Gravitino spark-connector,
 * which will cause unexpected exceptions if run RowLevelCommands using the Gravitino spark-connector.
 * Therefore it is necessary to override IcebergSparkSqlExtensionsParser,
 * so that RowLevelCommands can run normally using the Gravitino spark-connector.
 *
 * GravitinoIcebergSparkSqlExtensionsParser will be injected automatically in GravitinoIcebergSparkSessionExtensions,
 * and GravitinoIcebergSparkSessionExtensions will be automatically registered in GravitinoDriverPlugin.
 * Injecting GravitinoIcebergSparkSessionExtensions manually is not recommended,
 * because we need to ensure GravitinoIcebergSparkSessionExtensions is injected before IcebergSparkSessionExtensions
 * to avoid some unexpected exceptions.
 */
class GravitinoIcebergSparkSqlExtensionsParser(delegate: ParserInterface) extends IcebergSparkSqlExtensionsParser(delegate: ParserInterface) {

  import GravitinoIcebergSparkSqlExtensionsParser._

  private lazy val icebergSubstitutor = substitutorCtor.newInstance(SQLConf.get)
  private lazy val icebergAstBuilder = new IcebergSqlExtensionsAstBuilder(delegate)

  /**
   * Parse a string to a LogicalPlan.
   */
  override def parsePlan(sqlText: String): LogicalPlan = {
    val sqlTextAfterSubstitution = icebergSubstitutor.substitute(sqlText)
    if (isGravitinoIcebergCommand(sqlTextAfterSubstitution)) {
      parse(sqlTextAfterSubstitution) { parser => icebergAstBuilder.visit(parser.singleStatement()) }.asInstanceOf[LogicalPlan]
    } else {
      val parsedPlan = delegate.parsePlan(sqlText)
      parsedPlan match {
        case e: ExplainCommand =>
          e.copy(logicalPlan = replaceIcebergRowLevelCommands(e.logicalPlan))
        case p =>
          replaceIcebergRowLevelCommands(p)
      }
    }
  }

  // replaceIcebergRowLevelCommands is private in IcebergSparkSqlExtensionsParser, so it must be redefine here
  private def replaceIcebergRowLevelCommands(plan: LogicalPlan): LogicalPlan = plan resolveOperatorsDown {
    case UpdateTable(UnresolvedGravitinoIcebergTable(aliasedTable), assignments, condition) =>
      UpdateIcebergTable(aliasedTable, assignments, condition)

    case MergeIntoTable(UnresolvedGravitinoIcebergTable(aliasedTable), source, cond, matchedActions, notMatchedActions, Nil) =>
      // cannot construct MergeIntoIcebergTable right away as MERGE operations require special resolution
      // that's why the condition and actions must be hidden from the regular resolution rules in Spark
      // see ResolveMergeIntoTableReferences for details
      val context = MergeIntoContext(cond, matchedActions, notMatchedActions)
      UnresolvedMergeIntoIcebergTable(aliasedTable, source, context)

    case MergeIntoTable(UnresolvedGravitinoIcebergTable(_), _, _, _, _, notMatchedBySourceActions)
      if notMatchedBySourceActions.nonEmpty =>
      throw new AnalysisException("Iceberg does not support WHEN NOT MATCHED BY SOURCE clause")
  }

  object UnresolvedGravitinoIcebergTable {

    def unapply(plan: LogicalPlan): Option[LogicalPlan] = {
      EliminateSubqueryAliases(plan) match {
        case UnresolvedRelation(multipartIdentifier, _, _) if isSparkIcebergTable(multipartIdentifier) =>
          Some(plan)
        case _ =>
          None
      }
    }

    private def isSparkIcebergTable(multipartIdent: Seq[String]): Boolean = {
      val catalogAndIdentifier = Spark3Util.catalogAndIdentifier(SparkSession.active, multipartIdent.asJava)
      catalogAndIdentifier.catalog match {
        case tableCatalog: TableCatalog =>
          Try(tableCatalog.loadTable(catalogAndIdentifier.identifier))
            .map(isSparkIcebergTable)
            .getOrElse(false)

        case _ =>
          false
      }
    }

    // isSparkIcebergTable is private and defined in object UnresolvedIcebergTable of IcebergSparkSqlExtensionsParser,
    // so it must be redefine here
    private def isSparkIcebergTable(table: Table): Boolean = table match {
      case _: SparkIcebergTable => true
      case _ => false
    }
  }

  // isIcebergCommand is private in IcebergSparkSqlExtensionsParser, so it must be redefine here
  private def isGravitinoIcebergCommand(sqlText: String): Boolean = {
    val normalized = sqlText.toLowerCase(Locale.ROOT).trim()
      // Strip simple SQL comments that terminate a line, e.g. comments starting with `--` .
      .replaceAll("--.*?\\n", " ")
      // Strip newlines.
      .replaceAll("\\s+", " ")
      // Strip comments of the form  /* ... */. This must come after stripping newlines so that
      // comments that span multiple lines are caught.
      .replaceAll("/\\*.*?\\*/", " ")
      .trim()
    normalized.startsWith("call") || (
      normalized.startsWith("alter table") && (
        normalized.contains("add partition field") ||
          normalized.contains("drop partition field") ||
          normalized.contains("replace partition field") ||
          normalized.contains("write ordered by") ||
          normalized.contains("write locally ordered by") ||
          normalized.contains("write distributed by") ||
          normalized.contains("write unordered") ||
          normalized.contains("set identifier fields") ||
          normalized.contains("drop identifier fields") ||
          isGravitinoIcebergSnapshotRefDdl(normalized)))
  }

  // isSnapshotRefDdl is private in IcebergSparkSqlExtensionsParser, so it must be redefine here
  private def isGravitinoIcebergSnapshotRefDdl(normalized: String): Boolean = {
    normalized.contains("create branch") ||
      normalized.contains("replace branch") ||
      normalized.contains("create tag") ||
      normalized.contains("replace tag") ||
      normalized.contains("drop branch") ||
      normalized.contains("drop tag")
  }
}

object GravitinoIcebergSparkSqlExtensionsParser {
  private val substitutorCtor: DynConstructors.Ctor[VariableSubstitution] =
    DynConstructors.builder()
      .impl(classOf[VariableSubstitution])
      .impl(classOf[VariableSubstitution], classOf[SQLConf])
      .build()
}