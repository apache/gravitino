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

package org.apache.gravitino.kb;

import com.google.common.base.Preconditions;
import java.util.Arrays;
import java.util.Objects;
import org.apache.gravitino.annotation.Evolving;

/**
 * A change to a Knowledge Base Article. Changes to the title, the body, the Domains or the bindings
 * are versioned together: a request carrying any of them creates exactly one new version.
 *
 * <p>The article name is immutable, so no rename change exists.
 */
@Evolving
public interface KnowledgeBaseArticleChange {

  /**
   * Creates a change to replace the title.
   *
   * @param newTitle the new title
   * @return the change
   */
  static KnowledgeBaseArticleChange updateTitle(String newTitle) {
    return new UpdateTitle(newTitle);
  }

  /**
   * Creates a change to replace the body.
   *
   * @param newBody the new body
   * @return the change
   */
  static KnowledgeBaseArticleChange updateBody(ArticleBody newBody) {
    return new UpdateBody(newBody);
  }

  /**
   * Creates a change to replace the whole Domain list.
   *
   * @param newDomains the new Domain names
   * @return the change
   */
  static KnowledgeBaseArticleChange updateDomains(String[] newDomains) {
    return new UpdateDomains(newDomains);
  }

  /**
   * Creates a change to set the status.
   *
   * @param newStatus the new status
   * @return the change
   */
  static KnowledgeBaseArticleChange updateStatus(ArticleStatus newStatus) {
    return new UpdateStatus(newStatus);
  }

  /**
   * Creates a change to replace the comment. A {@code null} comment clears the field.
   *
   * @param newComment the new comment, or {@code null} to clear it
   * @return the change
   */
  static KnowledgeBaseArticleChange updateComment(String newComment) {
    return new UpdateComment(newComment);
  }

  /**
   * Creates a change to replace the whole synonym list.
   *
   * @param newSynonyms the new synonyms
   * @return the change
   */
  static KnowledgeBaseArticleChange updateSynonyms(String[] newSynonyms) {
    return new UpdateSynonyms(newSynonyms);
  }

  /**
   * Creates a change to replace the source reference. A {@code null} reference clears the field.
   *
   * @param newSourceReference the new source reference, or {@code null} to clear it
   * @return the change
   */
  static KnowledgeBaseArticleChange updateSourceReference(String newSourceReference) {
    return new UpdateSourceReference(newSourceReference);
  }

  /**
   * Creates a change to set a property.
   *
   * @param property the property name
   * @param value the property value
   * @return the change
   */
  static KnowledgeBaseArticleChange setProperty(String property, String value) {
    return new SetProperty(property, value);
  }

  /**
   * Creates a change to remove a property.
   *
   * @param property the property name
   * @return the change
   */
  static KnowledgeBaseArticleChange removeProperty(String property) {
    return new RemoveProperty(property);
  }

  /** A change to replace the title. */
  final class UpdateTitle implements KnowledgeBaseArticleChange {
    private final String newTitle;

    private UpdateTitle(String newTitle) {
      this.newTitle = ArticleValidation.title(newTitle);
    }

    /**
     * @return the new title
     */
    public String getNewTitle() {
      return newTitle;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      UpdateTitle that = (UpdateTitle) o;
      return Objects.equals(newTitle, that.newTitle);
    }

    @Override
    public int hashCode() {
      return Objects.hash(newTitle);
    }

    @Override
    public String toString() {
      return "UPDATETITLE " + newTitle;
    }
  }

  /** A change to replace the body. */
  final class UpdateBody implements KnowledgeBaseArticleChange {
    private final ArticleBody newBody;

    private UpdateBody(ArticleBody newBody) {
      this.newBody = Preconditions.checkNotNull(newBody, "body");
    }

    /**
     * @return the new body
     */
    public ArticleBody getNewBody() {
      return newBody;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      UpdateBody that = (UpdateBody) o;
      return Objects.equals(newBody, that.newBody);
    }

    @Override
    public int hashCode() {
      return Objects.hash(newBody);
    }

    @Override
    public String toString() {
      return "UPDATEBODY " + newBody;
    }
  }

  /** A change to replace the whole Domain list. */
  final class UpdateDomains implements KnowledgeBaseArticleChange {
    private final String[] newDomains;

    private UpdateDomains(String[] newDomains) {
      Preconditions.checkNotNull(newDomains, "newDomains");
      this.newDomains =
          ArticleValidation.domainNames(Arrays.asList(newDomains)).toArray(new String[0]);
    }

    /**
     * @return the new Domain names
     */
    public String[] getNewDomains() {
      return newDomains.clone();
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      UpdateDomains that = (UpdateDomains) o;
      return Arrays.equals(newDomains, that.newDomains);
    }

    @Override
    public int hashCode() {
      return Arrays.hashCode(newDomains);
    }

    @Override
    public String toString() {
      return "UPDATEDOMAINS " + Arrays.toString(newDomains);
    }
  }

  /** A change to set the status. */
  final class UpdateStatus implements KnowledgeBaseArticleChange {
    private final ArticleStatus newStatus;

    private UpdateStatus(ArticleStatus newStatus) {
      this.newStatus = Preconditions.checkNotNull(newStatus, "status");
    }

    /**
     * @return the new status
     */
    public ArticleStatus getNewStatus() {
      return newStatus;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      UpdateStatus that = (UpdateStatus) o;
      return newStatus == that.newStatus;
    }

    @Override
    public int hashCode() {
      return Objects.hash(newStatus);
    }

    @Override
    public String toString() {
      return "UPDATESTATUS " + newStatus;
    }
  }

  /** A change to replace the comment. */
  final class UpdateComment implements KnowledgeBaseArticleChange {
    private final String newComment;

    private UpdateComment(String newComment) {
      this.newComment =
          ArticleValidation.length(newComment, KnowledgeBaseArticle.COMMENT_MAX_LENGTH, "Comment");
    }

    /**
     * @return the new comment
     */
    public String getNewComment() {
      return newComment;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      UpdateComment that = (UpdateComment) o;
      return Objects.equals(newComment, that.newComment);
    }

    @Override
    public int hashCode() {
      return Objects.hash(newComment);
    }

    @Override
    public String toString() {
      return "UPDATECOMMENT " + newComment;
    }
  }

  /** A change to replace the whole synonym list. */
  final class UpdateSynonyms implements KnowledgeBaseArticleChange {
    private final String[] newSynonyms;

    private UpdateSynonyms(String[] newSynonyms) {
      Preconditions.checkNotNull(newSynonyms, "newSynonyms");
      this.newSynonyms =
          ArticleValidation.synonyms(Arrays.asList(newSynonyms)).toArray(new String[0]);
    }

    /**
     * @return the new synonyms
     */
    public String[] getNewSynonyms() {
      return newSynonyms.clone();
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      UpdateSynonyms that = (UpdateSynonyms) o;
      return Arrays.equals(newSynonyms, that.newSynonyms);
    }

    @Override
    public int hashCode() {
      return Arrays.hashCode(newSynonyms);
    }

    @Override
    public String toString() {
      return "UPDATESYNONYMS " + Arrays.toString(newSynonyms);
    }
  }

  /** A change to replace the source reference. */
  final class UpdateSourceReference implements KnowledgeBaseArticleChange {
    private final String newSourceReference;

    private UpdateSourceReference(String newSourceReference) {
      this.newSourceReference =
          ArticleValidation.length(
              newSourceReference,
              KnowledgeBaseArticle.SOURCE_REFERENCE_MAX_LENGTH,
              "Source reference");
    }

    /**
     * @return the new source reference
     */
    public String getNewSourceReference() {
      return newSourceReference;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      UpdateSourceReference that = (UpdateSourceReference) o;
      return Objects.equals(newSourceReference, that.newSourceReference);
    }

    @Override
    public int hashCode() {
      return Objects.hash(newSourceReference);
    }

    @Override
    public String toString() {
      return "UPDATESOURCEREFERENCE " + newSourceReference;
    }
  }

  /** A change to set a property. */
  final class SetProperty implements KnowledgeBaseArticleChange {
    private final String property;
    private final String value;

    private SetProperty(String property, String value) {
      this.property = property;
      this.value = value;
    }

    /**
     * @return the property name
     */
    public String getProperty() {
      return property;
    }

    /**
     * @return the property value
     */
    public String getValue() {
      return value;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      SetProperty that = (SetProperty) o;
      return Objects.equals(property, that.property) && Objects.equals(value, that.value);
    }

    @Override
    public int hashCode() {
      return Objects.hash(property, value);
    }

    @Override
    public String toString() {
      return "SETARTICLEPROPERTY " + property + " = " + value;
    }
  }

  /** A change to remove a property. */
  final class RemoveProperty implements KnowledgeBaseArticleChange {
    private final String property;

    private RemoveProperty(String property) {
      this.property = property;
    }

    /**
     * @return the property name
     */
    public String getProperty() {
      return property;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      RemoveProperty that = (RemoveProperty) o;
      return Objects.equals(property, that.property);
    }

    @Override
    public int hashCode() {
      return Objects.hash(property);
    }

    @Override
    public String toString() {
      return "REMOVEARTICLEPROPERTY " + property;
    }
  }
}
