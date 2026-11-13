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
package org.apache.gravitino.glossary;

import com.google.common.base.Preconditions;
import java.util.Arrays;
import java.util.Objects;
import javax.annotation.Nullable;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.annotation.Evolving;

/**
 * A change to a glossary term, passed to {@link GlossaryOperations#alterTerm(String,
 * TermChange...)}. All changes of one call are applied together.
 *
 * <p>Adding a synonym, domain, broader term or related term that the term already has, or removing
 * one that it does not have, is not an error and changes nothing.
 */
@Evolving
public interface TermChange {

  /**
   * Creates a change to rename the term. Bindings, relations and domains are kept.
   *
   * @param newName The new name of the term.
   * @return The term change.
   * @throws IllegalArgumentException If the new name is null or blank.
   */
  static TermChange rename(String newName) {
    Preconditions.checkArgument(StringUtils.isNotBlank(newName), "Term name cannot be blank");
    return new RenameTerm(newName);
  }

  /**
   * Creates a change to update the definition of the term.
   *
   * @param newDefinition The new definition, or null to remove it.
   * @return The term change.
   */
  static TermChange updateDefinition(@Nullable String newDefinition) {
    return new UpdateDefinition(newDefinition);
  }

  /**
   * Creates a change to set the status of the term. In Step 1, an authorized editor can move a term
   * from any status to any other without approval. This is not an approval decision: Step 2
   * publication requires its review workflow and cannot be bypassed with this change.
   *
   * @param newStatus The new status.
   * @return The term change.
   * @throws IllegalArgumentException If the new status is null.
   */
  static TermChange setStatus(GlossaryTerm.Status newStatus) {
    Preconditions.checkArgument(newStatus != null, "Term status cannot be null");
    return new SetStatus(newStatus);
  }

  /**
   * Creates a change to add a synonym to the term. A synonym cannot be the name of another term or
   * of this term.
   *
   * @param synonym The synonym to add.
   * @return The term change.
   * @throws IllegalArgumentException If the synonym is null or blank.
   */
  static TermChange addSynonym(String synonym) {
    Preconditions.checkArgument(StringUtils.isNotBlank(synonym), "Synonym cannot be blank");
    return new AddSynonym(synonym);
  }

  /**
   * Creates a change to remove a synonym from the term.
   *
   * @param synonym The synonym to remove.
   * @return The term change.
   * @throws IllegalArgumentException If the synonym is null or blank.
   */
  static TermChange removeSynonym(String synonym) {
    Preconditions.checkArgument(StringUtils.isNotBlank(synonym), "Synonym cannot be blank");
    return new RemoveSynonym(synonym);
  }

  /**
   * Creates a change to add the term to a domain.
   *
   * @param domain The name of the domain.
   * @return The term change.
   * @throws IllegalArgumentException If the domain name is null or blank.
   */
  static TermChange addDomain(String domain) {
    Preconditions.checkArgument(StringUtils.isNotBlank(domain), "Domain name cannot be blank");
    return new AddDomain(domain);
  }

  /**
   * Creates a change to remove the term from a domain.
   *
   * @param domain The name of the domain.
   * @return The term change.
   * @throws IllegalArgumentException If the domain name is null or blank.
   */
  static TermChange removeDomain(String domain) {
    Preconditions.checkArgument(StringUtils.isNotBlank(domain), "Domain name cannot be blank");
    return new RemoveDomain(domain);
  }

  /**
   * Creates a change to add a broader term. "A has broader term B" means "A is a kind of B". The
   * change fails if it would create a cycle, make a chain of broader terms longer than the depth
   * limit, or make a term an ancestor of one of its related terms.
   *
   * @param broader The name of the broader term.
   * @return The term change.
   * @throws IllegalArgumentException If the broader term name is null or blank.
   */
  static TermChange addBroader(String broader) {
    Preconditions.checkArgument(
        StringUtils.isNotBlank(broader), "Broader term name cannot be blank");
    return new AddBroader(broader);
  }

  /**
   * Creates a change to remove a broader term.
   *
   * @param broader The name of the broader term.
   * @return The term change.
   * @throws IllegalArgumentException If the broader term name is null or blank.
   */
  static TermChange removeBroader(String broader) {
    Preconditions.checkArgument(
        StringUtils.isNotBlank(broader), "Broader term name cannot be blank");
    return new RemoveBroader(broader);
  }

  /**
   * Creates a change to add a related term. The link shows on both terms. The change fails if one
   * term is an ancestor of the other.
   *
   * @param related The name of the related term.
   * @return The term change.
   * @throws IllegalArgumentException If the related term name is null or blank.
   */
  static TermChange addRelated(String related) {
    Preconditions.checkArgument(
        StringUtils.isNotBlank(related), "Related term name cannot be blank");
    return new AddRelated(related);
  }

  /**
   * Creates a change to remove a related term.
   *
   * @param related The name of the related term.
   * @return The term change.
   * @throws IllegalArgumentException If the related term name is null or blank.
   */
  static TermChange removeRelated(String related) {
    Preconditions.checkArgument(
        StringUtils.isNotBlank(related), "Related term name cannot be blank");
    return new RemoveRelated(related);
  }

  /**
   * Creates a change to replace all the documentation links of the term.
   *
   * @param links The new links, which replace all current links.
   * @return The term change.
   * @throws IllegalArgumentException If the array or one of its links is null.
   */
  static TermChange updateLinks(TermLink[] links) {
    Preconditions.checkArgument(links != null, "Links cannot be null");
    Preconditions.checkArgument(
        Arrays.stream(links).allMatch(Objects::nonNull), "A link cannot be null");
    return new UpdateLinks(links.clone());
  }

  /**
   * Creates a change to set the source reference of the term.
   *
   * @param sourceRef The source reference, in the form {@code <system>:<id>}, or null to remove it.
   * @return The term change.
   */
  static TermChange setSourceRef(@Nullable String sourceRef) {
    return new SetSourceRef(sourceRef);
  }

  /**
   * Creates a change to set a property of the term.
   *
   * @param property The property name.
   * @param value The property value.
   * @return The term change.
   * @throws IllegalArgumentException If the property name is null or blank, or the value is null.
   */
  static TermChange setProperty(String property, String value) {
    Preconditions.checkArgument(StringUtils.isNotBlank(property), "Property name cannot be blank");
    Preconditions.checkArgument(value != null, "Property value cannot be null");
    return new SetProperty(property, value);
  }

  /**
   * Creates a change to remove a property of the term.
   *
   * @param property The property name.
   * @return The term change.
   * @throws IllegalArgumentException If the property name is null or blank.
   */
  static TermChange removeProperty(String property) {
    Preconditions.checkArgument(StringUtils.isNotBlank(property), "Property name cannot be blank");
    return new RemoveProperty(property);
  }

  /** A term change created by {@link #rename}. */
  final class RenameTerm implements TermChange {
    private final String newName;

    private RenameTerm(String newName) {
      this.newName = newName;
    }

    /**
     * Returns the new name of the term.
     *
     * @return The new name of the term.
     */
    public String getNewName() {
      return newName;
    }

    @Override
    public boolean equals(@Nullable Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      RenameTerm that = (RenameTerm) o;
      return Objects.equals(newName, that.newName);
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(newName);
    }

    @Override
    public String toString() {
      return "RENAMETERM " + newName;
    }
  }

  /** A term change created by {@link #updateDefinition}. */
  final class UpdateDefinition implements TermChange {
    @Nullable private final String newDefinition;

    private UpdateDefinition(@Nullable String newDefinition) {
      this.newDefinition = newDefinition;
    }

    /**
     * Returns the new definition, or null to remove it.
     *
     * @return The new definition, or null to remove it.
     */
    @Nullable
    public String getNewDefinition() {
      return newDefinition;
    }

    @Override
    public boolean equals(@Nullable Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      UpdateDefinition that = (UpdateDefinition) o;
      return Objects.equals(newDefinition, that.newDefinition);
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(newDefinition);
    }

    @Override
    public String toString() {
      return "UPDATEDEFINITION " + newDefinition;
    }
  }

  /** A term change created by {@link #setStatus}. */
  final class SetStatus implements TermChange {
    private final GlossaryTerm.Status newStatus;

    private SetStatus(GlossaryTerm.Status newStatus) {
      this.newStatus = newStatus;
    }

    /**
     * Returns the new status.
     *
     * @return The new status.
     */
    public GlossaryTerm.Status getNewStatus() {
      return newStatus;
    }

    @Override
    public boolean equals(@Nullable Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      SetStatus that = (SetStatus) o;
      return newStatus == that.newStatus;
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(newStatus);
    }

    @Override
    public String toString() {
      return "SETSTATUS " + newStatus;
    }
  }

  /** A term change created by {@link #addSynonym}. */
  final class AddSynonym implements TermChange {
    private final String synonym;

    private AddSynonym(String synonym) {
      this.synonym = synonym;
    }

    /**
     * Returns the synonym to add.
     *
     * @return The synonym to add.
     */
    public String getSynonym() {
      return synonym;
    }

    @Override
    public boolean equals(@Nullable Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      AddSynonym that = (AddSynonym) o;
      return Objects.equals(synonym, that.synonym);
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(synonym);
    }

    @Override
    public String toString() {
      return "ADDSYNONYM " + synonym;
    }
  }

  /** A term change created by {@link #removeSynonym}. */
  final class RemoveSynonym implements TermChange {
    private final String synonym;

    private RemoveSynonym(String synonym) {
      this.synonym = synonym;
    }

    /**
     * Returns the synonym to remove.
     *
     * @return The synonym to remove.
     */
    public String getSynonym() {
      return synonym;
    }

    @Override
    public boolean equals(@Nullable Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      RemoveSynonym that = (RemoveSynonym) o;
      return Objects.equals(synonym, that.synonym);
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(synonym);
    }

    @Override
    public String toString() {
      return "REMOVESYNONYM " + synonym;
    }
  }

  /** A term change created by {@link #addDomain}. */
  final class AddDomain implements TermChange {
    private final String domain;

    private AddDomain(String domain) {
      this.domain = domain;
    }

    /**
     * Returns the name of the domain.
     *
     * @return The name of the domain.
     */
    public String getDomain() {
      return domain;
    }

    @Override
    public boolean equals(@Nullable Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      AddDomain that = (AddDomain) o;
      return Objects.equals(domain, that.domain);
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(domain);
    }

    @Override
    public String toString() {
      return "ADDDOMAIN " + domain;
    }
  }

  /** A term change created by {@link #removeDomain}. */
  final class RemoveDomain implements TermChange {
    private final String domain;

    private RemoveDomain(String domain) {
      this.domain = domain;
    }

    /**
     * Returns the name of the domain.
     *
     * @return The name of the domain.
     */
    public String getDomain() {
      return domain;
    }

    @Override
    public boolean equals(@Nullable Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      RemoveDomain that = (RemoveDomain) o;
      return Objects.equals(domain, that.domain);
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(domain);
    }

    @Override
    public String toString() {
      return "REMOVEDOMAIN " + domain;
    }
  }

  /** A term change created by {@link #addBroader}. */
  final class AddBroader implements TermChange {
    private final String broader;

    private AddBroader(String broader) {
      this.broader = broader;
    }

    /**
     * Returns the name of the broader term.
     *
     * @return The name of the broader term.
     */
    public String getBroader() {
      return broader;
    }

    @Override
    public boolean equals(@Nullable Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      AddBroader that = (AddBroader) o;
      return Objects.equals(broader, that.broader);
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(broader);
    }

    @Override
    public String toString() {
      return "ADDBROADER " + broader;
    }
  }

  /** A term change created by {@link #removeBroader}. */
  final class RemoveBroader implements TermChange {
    private final String broader;

    private RemoveBroader(String broader) {
      this.broader = broader;
    }

    /**
     * Returns the name of the broader term.
     *
     * @return The name of the broader term.
     */
    public String getBroader() {
      return broader;
    }

    @Override
    public boolean equals(@Nullable Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      RemoveBroader that = (RemoveBroader) o;
      return Objects.equals(broader, that.broader);
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(broader);
    }

    @Override
    public String toString() {
      return "REMOVEBROADER " + broader;
    }
  }

  /** A term change created by {@link #addRelated}. */
  final class AddRelated implements TermChange {
    private final String related;

    private AddRelated(String related) {
      this.related = related;
    }

    /**
     * Returns the name of the related term.
     *
     * @return The name of the related term.
     */
    public String getRelated() {
      return related;
    }

    @Override
    public boolean equals(@Nullable Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      AddRelated that = (AddRelated) o;
      return Objects.equals(related, that.related);
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(related);
    }

    @Override
    public String toString() {
      return "ADDRELATED " + related;
    }
  }

  /** A term change created by {@link #removeRelated}. */
  final class RemoveRelated implements TermChange {
    private final String related;

    private RemoveRelated(String related) {
      this.related = related;
    }

    /**
     * Returns the name of the related term.
     *
     * @return The name of the related term.
     */
    public String getRelated() {
      return related;
    }

    @Override
    public boolean equals(@Nullable Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      RemoveRelated that = (RemoveRelated) o;
      return Objects.equals(related, that.related);
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(related);
    }

    @Override
    public String toString() {
      return "REMOVERELATED " + related;
    }
  }

  /** A term change created by {@link #updateLinks}. */
  final class UpdateLinks implements TermChange {
    private final TermLink[] links;

    private UpdateLinks(TermLink[] links) {
      this.links = links;
    }

    /**
     * Returns the new links, which replace all current links.
     *
     * @return The new links, which replace all current links.
     */
    public TermLink[] getLinks() {
      return links.clone();
    }

    @Override
    public boolean equals(@Nullable Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      UpdateLinks that = (UpdateLinks) o;
      return Arrays.equals(links, that.links);
    }

    @Override
    public int hashCode() {
      return Arrays.hashCode(links);
    }

    @Override
    public String toString() {
      return "UPDATELINKS " + Arrays.toString(links);
    }
  }

  /** A term change created by {@link #setSourceRef}. */
  final class SetSourceRef implements TermChange {
    @Nullable private final String sourceRef;

    private SetSourceRef(@Nullable String sourceRef) {
      this.sourceRef = sourceRef;
    }

    /**
     * Returns the source reference, in the form {@code <system>:<id>}, or null to remove it.
     *
     * @return The source reference, in the form {@code <system>:<id>}, or null to remove it.
     */
    @Nullable
    public String getSourceRef() {
      return sourceRef;
    }

    @Override
    public boolean equals(@Nullable Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      SetSourceRef that = (SetSourceRef) o;
      return Objects.equals(sourceRef, that.sourceRef);
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(sourceRef);
    }

    @Override
    public String toString() {
      return "SETSOURCEREF " + sourceRef;
    }
  }

  /** A term change created by {@link #setProperty}. */
  final class SetProperty implements TermChange {
    private final String property;
    private final String value;

    private SetProperty(String property, String value) {
      this.property = property;
      this.value = value;
    }

    /**
     * Returns the property name.
     *
     * @return The property name.
     */
    public String getProperty() {
      return property;
    }

    /**
     * Returns the property value.
     *
     * @return The property value.
     */
    public String getValue() {
      return value;
    }

    @Override
    public boolean equals(@Nullable Object o) {
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
      return "SETPROPERTY " + property + " " + value;
    }
  }

  /** A term change created by {@link #removeProperty}. */
  final class RemoveProperty implements TermChange {
    private final String property;

    private RemoveProperty(String property) {
      this.property = property;
    }

    /**
     * Returns the property name.
     *
     * @return The property name.
     */
    public String getProperty() {
      return property;
    }

    @Override
    public boolean equals(@Nullable Object o) {
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
      return Objects.hashCode(property);
    }

    @Override
    public String toString() {
      return "REMOVEPROPERTY " + property;
    }
  }
}
