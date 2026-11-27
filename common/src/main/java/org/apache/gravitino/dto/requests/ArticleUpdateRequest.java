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
package org.apache.gravitino.dto.requests;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.google.common.base.Preconditions;
import java.util.Arrays;
import java.util.Locale;
import java.util.Objects;
import javax.annotation.Nullable;
import org.apache.gravitino.dto.kb.ArticleBodyDTO;
import org.apache.gravitino.dto.kb.ArticleObjectDTO;
import org.apache.gravitino.json.StrictJsonStringDeserializer;
import org.apache.gravitino.kb.ArticleStatus;
import org.apache.gravitino.kb.KnowledgeBaseArticleChange;

/** One typed edit in an atomic Knowledge Base Article update. */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "@type")
@JsonSubTypes({
  @JsonSubTypes.Type(value = ArticleUpdateRequest.UpdateTitle.class, name = "updateTitle"),
  @JsonSubTypes.Type(value = ArticleUpdateRequest.UpdateBody.class, name = "updateBody"),
  @JsonSubTypes.Type(value = ArticleUpdateRequest.UpdateDomains.class, name = "updateDomains"),
  @JsonSubTypes.Type(value = ArticleUpdateRequest.UpdateStatus.class, name = "updateStatus"),
  @JsonSubTypes.Type(value = ArticleUpdateRequest.UpdateComment.class, name = "updateComment"),
  @JsonSubTypes.Type(value = ArticleUpdateRequest.UpdateSynonyms.class, name = "updateSynonyms"),
  @JsonSubTypes.Type(
      value = ArticleUpdateRequest.UpdateSourceReference.class,
      name = "updateSourceReference"),
  @JsonSubTypes.Type(value = ArticleUpdateRequest.SetProperty.class, name = "setProperty"),
  @JsonSubTypes.Type(value = ArticleUpdateRequest.RemoveProperty.class, name = "removeProperty"),
  @JsonSubTypes.Type(value = ArticleUpdateRequest.AddTerms.class, name = "addTerms"),
  @JsonSubTypes.Type(value = ArticleUpdateRequest.RemoveTerms.class, name = "removeTerms"),
  @JsonSubTypes.Type(value = ArticleUpdateRequest.AddObjects.class, name = "addObjects"),
  @JsonSubTypes.Type(value = ArticleUpdateRequest.RemoveObjects.class, name = "removeObjects")
})
public interface ArticleUpdateRequest {
  /**
   * Returns the validated API change for this edit.
   *
   * @return the corresponding API change
   */
  KnowledgeBaseArticleChange toChange();

  /**
   * @param change an Article mutation
   * @return the corresponding REST update
   */
  static ArticleUpdateRequest from(KnowledgeBaseArticleChange change) {
    Preconditions.checkArgument(change != null, "Article change must not be null");
    if (change instanceof KnowledgeBaseArticleChange.UpdateTitle) {
      return new UpdateTitle(((KnowledgeBaseArticleChange.UpdateTitle) change).getNewTitle());
    }
    if (change instanceof KnowledgeBaseArticleChange.UpdateBody) {
      return new UpdateBody(
          ArticleBodyDTO.from(((KnowledgeBaseArticleChange.UpdateBody) change).getNewBody()));
    }
    if (change instanceof KnowledgeBaseArticleChange.UpdateDomains) {
      return new UpdateDomains(((KnowledgeBaseArticleChange.UpdateDomains) change).getNewDomains());
    }
    if (change instanceof KnowledgeBaseArticleChange.UpdateStatus) {
      return new UpdateStatus(
          ((KnowledgeBaseArticleChange.UpdateStatus) change).getNewStatus().name());
    }
    if (change instanceof KnowledgeBaseArticleChange.UpdateComment) {
      return new UpdateComment(((KnowledgeBaseArticleChange.UpdateComment) change).getNewComment());
    }
    if (change instanceof KnowledgeBaseArticleChange.UpdateSynonyms) {
      return new UpdateSynonyms(
          ((KnowledgeBaseArticleChange.UpdateSynonyms) change).getNewSynonyms());
    }
    if (change instanceof KnowledgeBaseArticleChange.UpdateSourceReference) {
      return new UpdateSourceReference(
          ((KnowledgeBaseArticleChange.UpdateSourceReference) change).getNewSourceReference());
    }
    if (change instanceof KnowledgeBaseArticleChange.SetProperty) {
      KnowledgeBaseArticleChange.SetProperty update =
          (KnowledgeBaseArticleChange.SetProperty) change;
      return new SetProperty(update.getProperty(), update.getValue());
    }
    if (change instanceof KnowledgeBaseArticleChange.RemoveProperty) {
      return new RemoveProperty(((KnowledgeBaseArticleChange.RemoveProperty) change).getProperty());
    }
    if (change instanceof KnowledgeBaseArticleChange.AddTerms) {
      return new AddTerms(
          ((KnowledgeBaseArticleChange.AddTerms) change).getTerms().toArray(new String[0]));
    }
    if (change instanceof KnowledgeBaseArticleChange.RemoveTerms) {
      return new RemoveTerms(
          ((KnowledgeBaseArticleChange.RemoveTerms) change).getTerms().toArray(new String[0]));
    }
    if (change instanceof KnowledgeBaseArticleChange.AddObjects) {
      return new AddObjects(
          ArticleObjectDTO.fromIdentifiers(
              ((KnowledgeBaseArticleChange.AddObjects) change).getObjects()));
    }
    if (change instanceof KnowledgeBaseArticleChange.RemoveObjects) {
      return new RemoveObjects(
          ArticleObjectDTO.fromIdentifiers(
              ((KnowledgeBaseArticleChange.RemoveObjects) change).getObjects()));
    }
    throw new IllegalArgumentException(
        "Unsupported Article change: " + change.getClass().getName());
  }

  /** Replaces the display title. */
  class UpdateTitle implements ArticleUpdateRequest {
    @JsonProperty("newTitle")
    @JsonDeserialize(using = StrictJsonStringDeserializer.class)
    private final String newTitle;

    /**
     * Creates the updateTitle change.
     *
     * @param newTitle the replacement title
     */
    @JsonCreator
    public UpdateTitle(@JsonProperty(value = "newTitle", required = true) String newTitle) {
      this.newTitle = newTitle;
    }

    @Override
    public KnowledgeBaseArticleChange toChange() {
      return KnowledgeBaseArticleChange.updateTitle(newTitle);
    }
  }

  /** Replaces the body and its markup format. */
  class UpdateBody implements ArticleUpdateRequest {
    @JsonProperty("newBody")
    private final ArticleBodyDTO newBody;

    /**
     * Creates the updateBody change.
     *
     * @param newBody the replacement body
     */
    @JsonCreator
    public UpdateBody(@JsonProperty(value = "newBody", required = true) ArticleBodyDTO newBody) {
      Preconditions.checkArgument(newBody != null, "newBody is required");
      this.newBody = newBody;
    }

    @Override
    public KnowledgeBaseArticleChange toChange() {
      return KnowledgeBaseArticleChange.updateBody(newBody.toBody());
    }
  }

  /** Replaces the visible Domain memberships. */
  class UpdateDomains implements ArticleUpdateRequest {
    @JsonProperty("newDomains")
    @JsonDeserialize(contentUsing = StrictJsonStringDeserializer.class)
    private final String[] newDomains;

    /**
     * Creates the updateDomains change.
     *
     * @param newDomains the replacement Domain names
     */
    @JsonCreator
    public UpdateDomains(@JsonProperty(value = "newDomains", required = true) String[] newDomains) {
      Preconditions.checkArgument(
          newDomains != null && Arrays.stream(newDomains).allMatch(Objects::nonNull),
          "newDomains must be an array without null elements");
      this.newDomains = newDomains.clone();
    }

    @Override
    public KnowledgeBaseArticleChange toChange() {
      return KnowledgeBaseArticleChange.updateDomains(newDomains);
    }
  }

  /** Sets the publication status. */
  class UpdateStatus implements ArticleUpdateRequest {
    @JsonProperty("newStatus")
    @JsonDeserialize(using = StrictJsonStringDeserializer.class)
    private final String newStatus;

    /**
     * Creates the updateStatus change.
     *
     * @param newStatus the requested status
     */
    @JsonCreator
    public UpdateStatus(@JsonProperty(value = "newStatus", required = true) String newStatus) {
      Preconditions.checkArgument(newStatus != null, "newStatus is required");
      this.newStatus = newStatus;
    }

    @Override
    public KnowledgeBaseArticleChange toChange() {
      return KnowledgeBaseArticleChange.updateStatus(
          ArticleStatus.valueOf(newStatus.toUpperCase(Locale.ROOT)));
    }
  }

  /** Updates or clears the Article comment. */
  class UpdateComment implements ArticleUpdateRequest {
    @JsonProperty("newComment")
    @Nullable
    @JsonInclude(JsonInclude.Include.ALWAYS)
    @JsonDeserialize(using = StrictJsonStringDeserializer.class)
    private final String newComment;

    /**
     * Creates the updateComment change.
     *
     * @param newComment the new comment, or null to clear it
     */
    @JsonCreator
    public UpdateComment(
        @JsonProperty(value = "newComment", required = true) @Nullable String newComment) {
      this.newComment = newComment;
    }

    @Override
    public KnowledgeBaseArticleChange toChange() {
      return KnowledgeBaseArticleChange.updateComment(newComment);
    }
  }

  /** Replaces the synonym list. */
  class UpdateSynonyms implements ArticleUpdateRequest {
    @JsonProperty("newSynonyms")
    @JsonDeserialize(contentUsing = StrictJsonStringDeserializer.class)
    private final String[] newSynonyms;

    /**
     * Creates the updateSynonyms change.
     *
     * @param newSynonyms the replacement synonyms
     */
    @JsonCreator
    public UpdateSynonyms(
        @JsonProperty(value = "newSynonyms", required = true) String[] newSynonyms) {
      Preconditions.checkArgument(
          newSynonyms != null && Arrays.stream(newSynonyms).allMatch(Objects::nonNull),
          "newSynonyms must be an array without null elements");
      this.newSynonyms = newSynonyms.clone();
    }

    @Override
    public KnowledgeBaseArticleChange toChange() {
      return KnowledgeBaseArticleChange.updateSynonyms(newSynonyms);
    }
  }

  /** Updates or clears the Article source reference. */
  class UpdateSourceReference implements ArticleUpdateRequest {
    @JsonProperty("newSourceReference")
    @Nullable
    @JsonInclude(JsonInclude.Include.ALWAYS)
    @JsonDeserialize(using = StrictJsonStringDeserializer.class)
    private final String newSourceReference;

    /**
     * Creates the updateSourceReference change.
     *
     * @param newSourceReference the source reference, or null to clear it
     */
    @JsonCreator
    public UpdateSourceReference(
        @JsonProperty(value = "newSourceReference", required = true) @Nullable
            String newSourceReference) {
      this.newSourceReference = newSourceReference;
    }

    @Override
    public KnowledgeBaseArticleChange toChange() {
      return KnowledgeBaseArticleChange.updateSourceReference(newSourceReference);
    }
  }

  /** Adds or updates a custom property. */
  class SetProperty implements ArticleUpdateRequest {
    @JsonProperty("property")
    @JsonDeserialize(using = StrictJsonStringDeserializer.class)
    private final String property;

    @JsonProperty("value")
    @JsonDeserialize(using = StrictJsonStringDeserializer.class)
    private final String value;

    /**
     * Creates the setProperty change.
     *
     * @param property the property name
     * @param value the property value
     */
    @JsonCreator
    public SetProperty(
        @JsonProperty(value = "property", required = true) String property,
        @JsonProperty(value = "value", required = true) String value) {
      this.property = property;
      this.value = value;
    }

    @Override
    public KnowledgeBaseArticleChange toChange() {
      return KnowledgeBaseArticleChange.setProperty(property, value);
    }
  }

  /** Removes a custom property. */
  class RemoveProperty implements ArticleUpdateRequest {
    @JsonProperty("property")
    @JsonDeserialize(using = StrictJsonStringDeserializer.class)
    private final String property;

    /**
     * Creates the removeProperty change.
     *
     * @param property the property name
     */
    @JsonCreator
    public RemoveProperty(@JsonProperty(value = "property", required = true) String property) {
      this.property = property;
    }

    @Override
    public KnowledgeBaseArticleChange toChange() {
      return KnowledgeBaseArticleChange.removeProperty(property);
    }
  }

  /** Adds Term bindings. */
  class AddTerms implements ArticleUpdateRequest {
    @JsonProperty("terms")
    @JsonDeserialize(contentUsing = StrictJsonStringDeserializer.class)
    private final String[] terms;

    /**
     * Creates the addTerms change.
     *
     * @param terms term names to bind
     */
    @JsonCreator
    public AddTerms(@JsonProperty(value = "terms", required = true) String[] terms) {
      Preconditions.checkArgument(
          terms != null && Arrays.stream(terms).allMatch(Objects::nonNull),
          "terms must be an array without null elements");
      this.terms = terms.clone();
    }

    @Override
    public KnowledgeBaseArticleChange toChange() {
      return KnowledgeBaseArticleChange.addTerms(terms);
    }
  }

  /** Removes Term bindings. */
  class RemoveTerms implements ArticleUpdateRequest {
    @JsonProperty("terms")
    @JsonDeserialize(contentUsing = StrictJsonStringDeserializer.class)
    private final String[] terms;

    /**
     * Creates the removeTerms change.
     *
     * @param terms term names to unbind
     */
    @JsonCreator
    public RemoveTerms(@JsonProperty(value = "terms", required = true) String[] terms) {
      Preconditions.checkArgument(
          terms != null && Arrays.stream(terms).allMatch(Objects::nonNull),
          "terms must be an array without null elements");
      this.terms = terms.clone();
    }

    @Override
    public KnowledgeBaseArticleChange toChange() {
      return KnowledgeBaseArticleChange.removeTerms(terms);
    }
  }

  /** Adds asset bindings. */
  class AddObjects implements ArticleUpdateRequest {
    @JsonProperty("objects")
    private final ArticleObjectDTO[] objects;

    /**
     * Creates the addObjects change.
     *
     * @param objects asset identifiers to bind
     */
    @JsonCreator
    public AddObjects(
        @JsonProperty(value = "objects", required = true) ArticleObjectDTO[] objects) {
      Preconditions.checkArgument(
          objects != null && Arrays.stream(objects).allMatch(Objects::nonNull),
          "objects must be an array without null elements");
      this.objects = objects.clone();
    }

    @Override
    public KnowledgeBaseArticleChange toChange() {
      return KnowledgeBaseArticleChange.addObjects(ArticleObjectDTO.toIdentifiers(objects));
    }
  }

  /** Removes asset bindings. */
  class RemoveObjects implements ArticleUpdateRequest {
    @JsonProperty("objects")
    private final ArticleObjectDTO[] objects;

    /**
     * Creates the removeObjects change.
     *
     * @param objects asset identifiers to unbind
     */
    @JsonCreator
    public RemoveObjects(
        @JsonProperty(value = "objects", required = true) ArticleObjectDTO[] objects) {
      Preconditions.checkArgument(
          objects != null && Arrays.stream(objects).allMatch(Objects::nonNull),
          "objects must be an array without null elements");
      this.objects = objects.clone();
    }

    @Override
    public KnowledgeBaseArticleChange toChange() {
      return KnowledgeBaseArticleChange.removeObjects(ArticleObjectDTO.toIdentifiers(objects));
    }
  }
}
