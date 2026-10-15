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
package org.apache.gravitino.json;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.common.base.Preconditions;
import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.gravitino.policy.IcebergDataCompactionContent;
import org.apache.gravitino.policy.IcebergOrphanFileRemovalContent;
import org.apache.gravitino.policy.IcebergRewriteManifestsContent;
import org.apache.gravitino.policy.IcebergSnapshotExpirationContent;
import org.apache.gravitino.policy.PolicyContents;
import org.apache.gravitino.policy.PolicyExecutionFields;
import org.apache.gravitino.policy.PolicyExecutionSpec;

/**
 * Jackson serdes for executable policy contents and {@link PolicyExecutionSpec}.
 *
 * <p>Follows the same idea as {@link JsonUtils.DistributionSerializer}: keep an immutable composed
 * value in memory, and map it to or from the flat wire/persistence keys ({@code timeTrigger},
 * {@code commitTrigger}, {@code minIntervalMs}, {@code jobOptions}, {@code updaterOptions}) without
 * nesting an {@code executionSpec} object.
 */
public final class ExecutablePolicyContentSerde {

  private ExecutablePolicyContentSerde() {}

  /**
   * Registers serdes used when persisting domain {@link org.apache.gravitino.policy.PolicyContent}
   * via {@link JsonUtils#anyFieldMapper()}.
   *
   * @param module module to register on
   */
  public static void registerDomainSerdes(SimpleModule module) {
    module.addSerializer(
        IcebergDataCompactionContent.class, new IcebergDataCompactionContentSerializer());
    module.addDeserializer(
        IcebergDataCompactionContent.class, new IcebergDataCompactionContentDeserializer());
    module.addSerializer(
        IcebergOrphanFileRemovalContent.class, new IcebergOrphanFileRemovalContentSerializer());
    module.addDeserializer(
        IcebergOrphanFileRemovalContent.class, new IcebergOrphanFileRemovalContentDeserializer());
    module.addSerializer(
        IcebergRewriteManifestsContent.class, new IcebergRewriteManifestsContentSerializer());
    module.addDeserializer(
        IcebergRewriteManifestsContent.class, new IcebergRewriteManifestsContentDeserializer());
    module.addSerializer(
        IcebergSnapshotExpirationContent.class, new IcebergSnapshotExpirationContentSerializer());
    module.addDeserializer(
        IcebergSnapshotExpirationContent.class, new IcebergSnapshotExpirationContentDeserializer());
  }

  /**
   * Writes execution peer fields as siblings of other content properties (no object wrapper).
   *
   * @param spec execution peers; {@code null} or empty writes nothing
   * @param gen JSON generator already inside a content object
   */
  public static void writeExecutionPeerFields(@Nullable PolicyExecutionSpec spec, JsonGenerator gen)
      throws IOException {
    if (spec == null || !spec.hasAny()) {
      return;
    }
    if (spec.timeTrigger() != null) {
      gen.writeStringField(PolicyExecutionFields.TIME_TRIGGER_JSON_KEY, spec.timeTrigger());
    }
    if (spec.commitTrigger() != null) {
      gen.writeBooleanField(PolicyExecutionFields.COMMIT_TRIGGER_JSON_KEY, spec.commitTrigger());
    }
    if (spec.minIntervalMs() != null) {
      gen.writeNumberField(PolicyExecutionFields.MIN_INTERVAL_MS_JSON_KEY, spec.minIntervalMs());
    }
    if (!spec.jobOptions().isEmpty()) {
      gen.writeObjectField(PolicyExecutionFields.JOB_OPTIONS_JSON_KEY, spec.jobOptions());
    }
    if (!spec.updaterOptions().isEmpty()) {
      gen.writeObjectField(PolicyExecutionFields.UPDATER_OPTIONS_JSON_KEY, spec.updaterOptions());
    }
  }

  /**
   * Reads flat execution peer keys from a content JSON object.
   *
   * @param node content object node
   * @return immutable spec (possibly {@link PolicyExecutionSpec#empty()})
   */
  public static PolicyExecutionSpec readExecutionSpec(JsonNode node) {
    Preconditions.checkArgument(
        node != null && node.isObject(),
        "Cannot parse execution peers from invalid JSON: %s",
        node);
    String timeTrigger = textOrNull(node, PolicyExecutionFields.TIME_TRIGGER_JSON_KEY);
    Boolean commitTrigger = booleanOrNull(node, PolicyExecutionFields.COMMIT_TRIGGER_JSON_KEY);
    Long minIntervalMs = longOrNull(node, PolicyExecutionFields.MIN_INTERVAL_MS_JSON_KEY);
    Map<String, String> jobOptions =
        stringMapOrNull(node, PolicyExecutionFields.JOB_OPTIONS_JSON_KEY);
    Map<String, String> updaterOptions =
        stringMapOrNull(node, PolicyExecutionFields.UPDATER_OPTIONS_JSON_KEY);
    return PolicyExecutionSpec.of(
        timeTrigger, commitTrigger, minIntervalMs, jobOptions, updaterOptions);
  }

  /**
   * Serializer for {@link PolicyExecutionSpec} as a JSON object with the flat peer property names.
   *
   * <p>Same role as {@link JsonUtils.DistributionSerializer}: owns the wire shape of the composed
   * value. Domain content serdes call {@link #writeExecutionPeerFields} so peers stay un-nested on
   * the content object.
   */
  public static class PolicyExecutionSpecSerializer extends JsonSerializer<PolicyExecutionSpec> {
    @Override
    public void serialize(
        PolicyExecutionSpec value, JsonGenerator gen, SerializerProvider serializers)
        throws IOException {
      gen.writeStartObject();
      writeExecutionPeerFields(value, gen);
      gen.writeEndObject();
    }
  }

  /**
   * Deserializer for {@link PolicyExecutionSpec} from a JSON object with the flat peer property
   * names.
   */
  public static class PolicyExecutionSpecDeserializer
      extends JsonDeserializer<PolicyExecutionSpec> {
    @Override
    public PolicyExecutionSpec deserialize(JsonParser p, DeserializationContext ctxt)
        throws IOException {
      JsonNode node = p.getCodec().readTree(p);
      return readExecutionSpec(node);
    }
  }

  /** Domain persistence serde for Iceberg compaction content. */
  public static class IcebergDataCompactionContentSerializer
      extends JsonSerializer<IcebergDataCompactionContent> {
    @Override
    public void serialize(
        IcebergDataCompactionContent value, JsonGenerator gen, SerializerProvider serializers)
        throws IOException {
      gen.writeStartObject();
      gen.writeNumberField("minDataFileMse", value.minDataFileMse());
      gen.writeNumberField("minDeleteFileNumber", value.minDeleteFileNumber());
      gen.writeNumberField("dataFileMseWeight", value.dataFileMseWeight());
      gen.writeNumberField("deleteFileNumberWeight", value.deleteFileNumberWeight());
      gen.writeNumberField("maxPartitionNum", value.maxPartitionNum());
      gen.writeStringField("rewriteStrategy", value.rewriteStrategy());
      gen.writeStringField("sortOrder", value.sortOrder());
      if (value.rewriteOptions() != null && !value.rewriteOptions().isEmpty()) {
        gen.writeObjectField("rewriteOptions", value.rewriteOptions());
      }
      writeExecutionPeerFields(value.executionSpec(), gen);
      gen.writeEndObject();
    }
  }

  /** Domain persistence serde for Iceberg compaction content. */
  public static class IcebergDataCompactionContentDeserializer
      extends JsonDeserializer<IcebergDataCompactionContent> {
    @Override
    public IcebergDataCompactionContent deserialize(JsonParser p, DeserializationContext ctxt)
        throws IOException {
      JsonNode node = p.getCodec().readTree(p);
      Preconditions.checkArgument(
          node != null && node.isObject(),
          "Cannot parse iceberg compaction content from invalid JSON: %s",
          node);
      return (IcebergDataCompactionContent)
          PolicyContents.icebergDataCompaction(
              longOrNull(node, "minDataFileMse"),
              longOrNull(node, "minDeleteFileNumber"),
              longOrNull(node, "dataFileMseWeight"),
              longOrNull(node, "deleteFileNumberWeight"),
              longOrNull(node, "maxPartitionNum"),
              textOrNull(node, "rewriteStrategy"),
              textOrNull(node, "sortOrder"),
              stringMapOrNull(node, "rewriteOptions"),
              textOrNull(node, PolicyExecutionFields.TIME_TRIGGER_JSON_KEY),
              booleanOrNull(node, PolicyExecutionFields.COMMIT_TRIGGER_JSON_KEY),
              longOrNull(node, PolicyExecutionFields.MIN_INTERVAL_MS_JSON_KEY),
              stringMapOrNull(node, PolicyExecutionFields.JOB_OPTIONS_JSON_KEY),
              stringMapOrNull(node, PolicyExecutionFields.UPDATER_OPTIONS_JSON_KEY));
    }
  }

  /** Domain persistence serde for orphan-file removal content. */
  public static class IcebergOrphanFileRemovalContentSerializer
      extends JsonSerializer<IcebergOrphanFileRemovalContent> {
    @Override
    public void serialize(
        IcebergOrphanFileRemovalContent value, JsonGenerator gen, SerializerProvider serializers)
        throws IOException {
      gen.writeStartObject();
      gen.writeNumberField("olderThanDays", value.olderThanDays());
      if (value.location() != null) {
        gen.writeStringField("location", value.location());
      }
      gen.writeBooleanField("dryRun", value.dryRun());
      writeExecutionPeerFields(value.executionSpec(), gen);
      gen.writeEndObject();
    }
  }

  /** Domain persistence serde for orphan-file removal content. */
  public static class IcebergOrphanFileRemovalContentDeserializer
      extends JsonDeserializer<IcebergOrphanFileRemovalContent> {
    @Override
    public IcebergOrphanFileRemovalContent deserialize(JsonParser p, DeserializationContext ctxt)
        throws IOException {
      JsonNode node = p.getCodec().readTree(p);
      Preconditions.checkArgument(
          node != null && node.isObject(),
          "Cannot parse orphan file removal content from invalid JSON: %s",
          node);
      Long olderThanDaysNode = longOrNull(node, "olderThanDays");
      long olderThanDays =
          olderThanDaysNode == null
              ? IcebergOrphanFileRemovalContent.DEFAULT_OLDER_THAN_DAYS
              : olderThanDaysNode;
      Boolean dryRunNode = booleanOrNull(node, "dryRun");
      boolean dryRun =
          dryRunNode == null ? IcebergOrphanFileRemovalContent.DEFAULT_DRY_RUN : dryRunNode;
      return (IcebergOrphanFileRemovalContent)
          PolicyContents.icebergOrphanFileRemoval(
              olderThanDays,
              textOrNull(node, "location"),
              dryRun,
              textOrNull(node, PolicyExecutionFields.TIME_TRIGGER_JSON_KEY),
              booleanOrNull(node, PolicyExecutionFields.COMMIT_TRIGGER_JSON_KEY),
              longOrNull(node, PolicyExecutionFields.MIN_INTERVAL_MS_JSON_KEY),
              stringMapOrNull(node, PolicyExecutionFields.JOB_OPTIONS_JSON_KEY),
              stringMapOrNull(node, PolicyExecutionFields.UPDATER_OPTIONS_JSON_KEY));
    }
  }

  /** Domain persistence serde for rewrite-manifests content. */
  public static class IcebergRewriteManifestsContentSerializer
      extends JsonSerializer<IcebergRewriteManifestsContent> {
    @Override
    public void serialize(
        IcebergRewriteManifestsContent value, JsonGenerator gen, SerializerProvider serializers)
        throws IOException {
      gen.writeStartObject();
      writeExecutionPeerFields(value.executionSpec(), gen);
      gen.writeEndObject();
    }
  }

  /** Domain persistence serde for rewrite-manifests content. */
  public static class IcebergRewriteManifestsContentDeserializer
      extends JsonDeserializer<IcebergRewriteManifestsContent> {
    @Override
    public IcebergRewriteManifestsContent deserialize(JsonParser p, DeserializationContext ctxt)
        throws IOException {
      JsonNode node = p.getCodec().readTree(p);
      Preconditions.checkArgument(
          node != null && node.isObject(),
          "Cannot parse rewrite-manifests content from invalid JSON: %s",
          node);
      return (IcebergRewriteManifestsContent)
          PolicyContents.icebergRewriteManifests(
              textOrNull(node, PolicyExecutionFields.TIME_TRIGGER_JSON_KEY),
              booleanOrNull(node, PolicyExecutionFields.COMMIT_TRIGGER_JSON_KEY),
              longOrNull(node, PolicyExecutionFields.MIN_INTERVAL_MS_JSON_KEY),
              stringMapOrNull(node, PolicyExecutionFields.JOB_OPTIONS_JSON_KEY),
              stringMapOrNull(node, PolicyExecutionFields.UPDATER_OPTIONS_JSON_KEY));
    }
  }

  /** Domain persistence serde for snapshot-expiration content. */
  public static class IcebergSnapshotExpirationContentSerializer
      extends JsonSerializer<IcebergSnapshotExpirationContent> {
    @Override
    public void serialize(
        IcebergSnapshotExpirationContent value, JsonGenerator gen, SerializerProvider serializers)
        throws IOException {
      gen.writeStartObject();
      writeExecutionPeerFields(value.executionSpec(), gen);
      gen.writeEndObject();
    }
  }

  /** Domain persistence serde for snapshot-expiration content. */
  public static class IcebergSnapshotExpirationContentDeserializer
      extends JsonDeserializer<IcebergSnapshotExpirationContent> {
    @Override
    public IcebergSnapshotExpirationContent deserialize(JsonParser p, DeserializationContext ctxt)
        throws IOException {
      JsonNode node = p.getCodec().readTree(p);
      Preconditions.checkArgument(
          node != null && node.isObject(),
          "Cannot parse snapshot-expiration content from invalid JSON: %s",
          node);
      return (IcebergSnapshotExpirationContent)
          PolicyContents.icebergSnapshotExpiration(
              textOrNull(node, PolicyExecutionFields.TIME_TRIGGER_JSON_KEY),
              booleanOrNull(node, PolicyExecutionFields.COMMIT_TRIGGER_JSON_KEY),
              longOrNull(node, PolicyExecutionFields.MIN_INTERVAL_MS_JSON_KEY),
              stringMapOrNull(node, PolicyExecutionFields.JOB_OPTIONS_JSON_KEY),
              stringMapOrNull(node, PolicyExecutionFields.UPDATER_OPTIONS_JSON_KEY));
    }
  }

  @Nullable
  private static String textOrNull(JsonNode node, String property) {
    if (!node.has(property) || node.get(property).isNull()) {
      return null;
    }
    return node.get(property).asText();
  }

  @Nullable
  private static Boolean booleanOrNull(JsonNode node, String property) {
    if (!node.has(property) || node.get(property).isNull()) {
      return null;
    }
    return node.get(property).asBoolean();
  }

  @Nullable
  private static Long longOrNull(JsonNode node, String property) {
    if (!node.has(property) || node.get(property).isNull()) {
      return null;
    }
    return node.get(property).asLong();
  }

  @Nullable
  private static Map<String, String> stringMapOrNull(JsonNode node, String property) {
    if (!node.has(property) || node.get(property).isNull()) {
      return null;
    }
    JsonNode mapNode = node.get(property);
    Preconditions.checkArgument(
        mapNode.isObject(), "Cannot parse string map property %s from: %s", property, mapNode);
    Map<String, String> map = new LinkedHashMap<>();
    Iterator<Map.Entry<String, JsonNode>> fields = mapNode.fields();
    while (fields.hasNext()) {
      Map.Entry<String, JsonNode> entry = fields.next();
      map.put(entry.getKey(), entry.getValue().isNull() ? null : entry.getValue().asText());
    }
    return map;
  }
}
