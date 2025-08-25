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
package org.apache.gravitino.client;

import static org.apache.gravitino.dto.util.DTOConverters.fromDTO;
import static org.apache.gravitino.dto.util.DTOConverters.toDTO;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import lombok.SneakyThrows;
import org.apache.gravitino.Audit;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.MetadataObjects;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.authorization.SupportsRoles;
import org.apache.gravitino.dto.rel.TableDTO;
import org.apache.gravitino.dto.rel.partitions.PartitionDTO;
import org.apache.gravitino.dto.requests.AddPartitionsRequest;
import org.apache.gravitino.dto.responses.DropResponse;
import org.apache.gravitino.dto.responses.PartitionListResponse;
import org.apache.gravitino.dto.responses.PartitionNameListResponse;
import org.apache.gravitino.dto.responses.PartitionResponse;
import org.apache.gravitino.exceptions.IllegalStatisticNameException;
import org.apache.gravitino.exceptions.NoSuchPartitionException;
import org.apache.gravitino.exceptions.NoSuchPolicyException;
import org.apache.gravitino.exceptions.NoSuchTagException;
import org.apache.gravitino.exceptions.PartitionAlreadyExistsException;
import org.apache.gravitino.exceptions.PolicyAlreadyAssociatedException;
import org.apache.gravitino.exceptions.UnmodifiableStatisticException;
import org.apache.gravitino.policy.Policy;
import org.apache.gravitino.policy.SupportsPolicies;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.SupportsPartitions;
import org.apache.gravitino.rel.Table;
import org.apache.gravitino.rel.expressions.distributions.Distribution;
import org.apache.gravitino.rel.expressions.sorts.SortOrder;
import org.apache.gravitino.rel.expressions.transforms.Transform;
import org.apache.gravitino.rel.indexes.Index;
import org.apache.gravitino.rel.partitions.Partition;
import org.apache.gravitino.rest.RESTUtils;
import org.apache.gravitino.stats.PartitionRange;
import org.apache.gravitino.stats.PartitionStatistics;
import org.apache.gravitino.stats.PartitionStatisticsDrop;
import org.apache.gravitino.stats.PartitionStatisticsUpdate;
import org.apache.gravitino.stats.Statistic;
import org.apache.gravitino.stats.StatisticValue;
import org.apache.gravitino.stats.SupportsPartitionStatistics;
import org.apache.gravitino.stats.SupportsStatistics;
import org.apache.gravitino.tag.SupportsTags;
import org.apache.gravitino.tag.Tag;

/** Represents a relational table. */
class RelationalTable
    implements Table,
        SupportsPartitions,
        SupportsTags,
        SupportsRoles,
        SupportsPolicies,
        SupportsStatistics,
        SupportsPartitionStatistics {

  private static final Joiner DOT_JOINER = Joiner.on(".");

  private final Table table;

  private final RESTClient restClient;

  private final Namespace namespace;

  private final MetadataObjectTagOperations objectTagOperations;
  private final MetadataObjectRoleOperations objectRoleOperations;
  private final MetadataObjectPolicyOperations objectPolicyOperations;
  private final MetadataObjectStatisticsOperations objectStatisticsOperations;
  private final MetadataObjectPartitionStatisticsOperations objectPartitionStatisticsOperations;

  /**
   * Creates a new RelationalTable.
   *
   * @param namespace The full namespace of the table, including metalake, catalog and schema.
   * @param tableDTO The table data transfer object.
   * @param restClient The REST client.
   * @return A new RelationalTable.
   */
  static RelationalTable from(Namespace namespace, TableDTO tableDTO, RESTClient restClient) {
    return new RelationalTable(namespace, tableDTO, restClient);
  }

  /**
   * Creates a new RelationalTable.
   *
   * @param namespace The full namespace of the table, including metalake, catalog and schema
   * @param tableDTO The table data transfer object.
   * @param restClient The REST client.
   */
  private RelationalTable(Namespace namespace, TableDTO tableDTO, RESTClient restClient) {
    this.namespace = namespace;
    this.restClient = restClient;
    this.table = fromDTO(tableDTO);
    MetadataObject tableObject =
        MetadataObjects.parse(tableFullName(namespace, tableDTO.name()), MetadataObject.Type.TABLE);
    this.objectTagOperations =
        new MetadataObjectTagOperations(namespace.level(0), tableObject, restClient);
    this.objectRoleOperations =
        new MetadataObjectRoleOperations(namespace.level(0), tableObject, restClient);
    this.objectPolicyOperations =
        new MetadataObjectPolicyOperations(namespace.level(0), tableObject, restClient);
    this.objectStatisticsOperations =
        new MetadataObjectStatisticsOperations(namespace.level(0), tableObject, restClient);
    this.objectPartitionStatisticsOperations =
        new MetadataObjectPartitionStatisticsOperations(
            namespace.level(0), tableObject, restClient);
  }

  /**
   * Returns the name of the table.
   *
   * @return The name of the table.
   */
  @Override
  public String name() {
    return table.name();
  }

  /** @return the columns of the table. */
  @Override
  public Column[] columns() {
    return Arrays.stream(table.columns())
        .map(
            c ->
                new GenericColumn(
                    c,
                    restClient,
                    namespace.level(0),
                    namespace.level(1),
                    namespace.level(2),
                    name()))
        .toArray(Column[]::new);
  }

  /** @return the partitioning of the table. */
  @Override
  public Transform[] partitioning() {
    return table.partitioning();
  }

  /** @return the sort order of the table. */
  @Override
  public SortOrder[] sortOrder() {
    return table.sortOrder();
  }

  /** @return the distribution of the table. */
  @Override
  public Distribution distribution() {
    return table.distribution();
  }

  /** @return the comment of the table. */
  @Nullable
  @Override
  public String comment() {
    return table.comment();
  }

  /** @return the properties of the table. */
  @Override
  public Map<String, String> properties() {
    return table.properties();
  }

  /** @return the audit information of the table. */
  @Override
  public Audit auditInfo() {
    return table.auditInfo();
  }

  /** @return the indexes of the table. */
  @Override
  public Index[] index() {
    return table.index();
  }

  /** @return The partition names of the table. */
  @Override
  public String[] listPartitionNames() {
    PartitionNameListResponse resp =
        restClient.get(
            getPartitionRequestPath(),
            PartitionNameListResponse.class,
            Collections.emptyMap(),
            ErrorHandlers.partitionErrorHandler());
    return resp.partitionNames();
  }

  /** @return The partition request path. */
  @VisibleForTesting
  String getPartitionRequestPath() {
    return "api/metalakes/"
        + RESTUtils.encodeString(namespace.level(0))
        + "/catalogs/"
        + RESTUtils.encodeString(namespace.level(1))
        + "/schemas/"
        + RESTUtils.encodeString(namespace.level(2))
        + "/tables/"
        + RESTUtils.encodeString(name())
        + "/partitions";
  }

  /** @return The partitions of the table. */
  @Override
  public Partition[] listPartitions() {
    Map<String, String> params = new HashMap<>();
    params.put("details", "true");
    PartitionListResponse resp =
        restClient.get(
            getPartitionRequestPath(),
            params,
            PartitionListResponse.class,
            Collections.emptyMap(),
            ErrorHandlers.partitionErrorHandler());
    return resp.getPartitions();
  }

  /**
   * Returns the partition with the given name.
   *
   * @param partitionName the name of the partition
   * @return the partition with the given name
   * @throws NoSuchPartitionException if the partition does not exist, throws this exception.
   */
  @Override
  public Partition getPartition(String partitionName) throws NoSuchPartitionException {
    PartitionResponse resp =
        restClient.get(
            formatPartitionRequestPath(getPartitionRequestPath(), partitionName),
            PartitionResponse.class,
            Collections.emptyMap(),
            ErrorHandlers.partitionErrorHandler());
    return resp.getPartition();
  }

  /**
   * Adds a partition to the table.
   *
   * @param partition The partition to add.
   * @return The added partition.
   * @throws PartitionAlreadyExistsException If the partition already exists, throws this exception.
   */
  @Override
  public Partition addPartition(Partition partition) throws PartitionAlreadyExistsException {
    AddPartitionsRequest req = new AddPartitionsRequest(new PartitionDTO[] {toDTO(partition)});
    req.validate();

    PartitionListResponse resp =
        restClient.post(
            getPartitionRequestPath(),
            req,
            PartitionListResponse.class,
            Collections.emptyMap(),
            ErrorHandlers.partitionErrorHandler());
    resp.validate();

    return resp.getPartitions()[0];
  }

  /**
   * Drops the partition with the given name.
   *
   * @param partitionName The name of the partition.
   * @return true if the partition is dropped, false if the partition does not exist.
   */
  @Override
  public boolean dropPartition(String partitionName) {
    DropResponse resp =
        restClient.delete(
            formatPartitionRequestPath(getPartitionRequestPath(), partitionName),
            DropResponse.class,
            Collections.emptyMap(),
            ErrorHandlers.partitionErrorHandler());
    resp.validate();
    return resp.dropped();
  }

  /**
   * Returns the partitioning strategy of the table.
   *
   * @return the partitioning strategy of the table.
   */
  @Override
  public SupportsPartitions supportPartitions() throws UnsupportedOperationException {
    return this;
  }

  /**
   * Formats the partition request path.
   *
   * @param prefix The prefix of the path.
   * @param partitionName The name of the partition.
   * @return The formatted partition request path.
   */
  @VisibleForTesting
  @SneakyThrows // Encode charset is fixed to UTF-8, so this is safe.
  protected static String formatPartitionRequestPath(String prefix, String partitionName) {
    return prefix + "/" + RESTUtils.encodeString(partitionName);
  }

  @Override
  public SupportsTags supportsTags() {
    return this;
  }

  @Override
  public SupportsPolicies supportsPolicies() {
    return this;
  }

  @Override
  public SupportsRoles supportsRoles() {
    return this;
  }

  @Override
  public SupportsStatistics supportsStatistics() {
    return this;
  }

  @Override
  public SupportsPartitionStatistics supportsPartitionStatistics() {
    return this;
  }

  private static String tableFullName(Namespace tableNS, String tableName) {
    return DOT_JOINER.join(tableNS.level(1), tableNS.level(2), tableName);
  }

  @Override
  public String[] listTags() {
    return objectTagOperations.listTags();
  }

  @Override
  public Tag[] listTagsInfo() {
    return objectTagOperations.listTagsInfo();
  }

  @Override
  public Tag getTag(String name) throws NoSuchTagException {
    return objectTagOperations.getTag(name);
  }

  @Override
  public String[] associateTags(String[] tagsToAdd, String[] tagsToRemove) {
    return objectTagOperations.associateTags(tagsToAdd, tagsToRemove);
  }

  @Override
  public String[] listPolicies() {
    return objectPolicyOperations.listPolicies();
  }

  @Override
  public Policy[] listPolicyInfos() {
    return objectPolicyOperations.listPolicyInfos();
  }

  @Override
  public Policy getPolicy(String name) throws NoSuchPolicyException {
    return objectPolicyOperations.getPolicy(name);
  }

  @Override
  public String[] associatePolicies(String[] policiesToAdd, String[] policiesToRemove)
      throws PolicyAlreadyAssociatedException {
    return objectPolicyOperations.associatePolicies(policiesToAdd, policiesToRemove);
  }

  @Override
  public String[] listBindingRoleNames() {
    return objectRoleOperations.listBindingRoleNames();
  }

  @Override
  public List<Statistic> listStatistics() {
    return objectStatisticsOperations.listStatistics();
  }

  @Override
  public void updateStatistics(Map<String, StatisticValue<?>> statistics)
      throws UnmodifiableStatisticException, IllegalStatisticNameException {
    objectStatisticsOperations.updateStatistics(statistics);
  }

  @Override
  public boolean dropStatistics(List<String> statistics) throws UnmodifiableStatisticException {
    return objectStatisticsOperations.dropStatistics(statistics);
  }

  @Override
  public List<PartitionStatistics> listPartitionStatistics(PartitionRange range) {
    return objectPartitionStatisticsOperations.listPartitionStatistics(range);
  }

  @Override
  public void updatePartitionStatistics(List<PartitionStatisticsUpdate> statisticsToUpdate)
      throws UnmodifiableStatisticException {
    objectPartitionStatisticsOperations.updatePartitionStatistics(statisticsToUpdate);
  }

  @Override
  public boolean dropPartitionStatistics(List<PartitionStatisticsDrop> statisticsToDrop)
      throws UnmodifiableStatisticException {
    return objectPartitionStatisticsOperations.dropPartitionStatistics(statisticsToDrop);
  }
}
