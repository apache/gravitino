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

import static org.apache.gravitino.dto.util.DTOConverters.toFunctionArg;

import org.apache.gravitino.Catalog;
import org.apache.gravitino.CatalogChange;
import org.apache.gravitino.MetalakeChange;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.SchemaChange;
import org.apache.gravitino.authorization.SecurableObject;
import org.apache.gravitino.dto.AuditDTO;
import org.apache.gravitino.dto.CatalogDTO;
import org.apache.gravitino.dto.MetalakeDTO;
import org.apache.gravitino.dto.authorization.PrivilegeDTO;
import org.apache.gravitino.dto.authorization.SecurableObjectDTO;
import org.apache.gravitino.dto.requests.CatalogUpdateRequest;
import org.apache.gravitino.dto.requests.FilesetUpdateRequest;
import org.apache.gravitino.dto.requests.MetalakeUpdateRequest;
import org.apache.gravitino.dto.requests.SchemaUpdateRequest;
import org.apache.gravitino.dto.requests.TableUpdateRequest;
import org.apache.gravitino.dto.requests.TagUpdateRequest;
import org.apache.gravitino.dto.requests.TopicUpdateRequest;
import org.apache.gravitino.file.FilesetChange;
import org.apache.gravitino.messaging.TopicChange;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.TableChange;
import org.apache.gravitino.rel.expressions.Expression;
import org.apache.gravitino.tag.TagChange;

class DTOConverters {
  private DTOConverters() {}

  static GravitinoMetalake toMetaLake(MetalakeDTO metalake, RESTClient client) {
    return GravitinoMetalake.builder()
        .withName(metalake.name())
        .withComment(metalake.comment())
        .withProperties(metalake.properties())
        .withAudit((AuditDTO) metalake.auditInfo())
        .withRestClient(client)
        .build();
  }

  static MetalakeUpdateRequest toMetalakeUpdateRequest(MetalakeChange change) {
    if (change instanceof MetalakeChange.RenameMetalake) {
      return new MetalakeUpdateRequest.RenameMetalakeRequest(
          ((MetalakeChange.RenameMetalake) change).getNewName());

    } else if (change instanceof MetalakeChange.UpdateMetalakeComment) {
      return new MetalakeUpdateRequest.UpdateMetalakeCommentRequest(
          ((MetalakeChange.UpdateMetalakeComment) change).getNewComment());

    } else if (change instanceof MetalakeChange.SetProperty) {
      return new MetalakeUpdateRequest.SetMetalakePropertyRequest(
          ((MetalakeChange.SetProperty) change).getProperty(),
          ((MetalakeChange.SetProperty) change).getValue());

    } else if (change instanceof MetalakeChange.RemoveProperty) {
      return new MetalakeUpdateRequest.RemoveMetalakePropertyRequest(
          ((MetalakeChange.RemoveProperty) change).getProperty());

    } else {
      throw new IllegalArgumentException(
          "Unknown change type: " + change.getClass().getSimpleName());
    }
  }

  @SuppressWarnings("unchecked")
  static Catalog toCatalog(String metalake, CatalogDTO catalog, RESTClient client) {
    Namespace namespace = Namespace.of(metalake);
    switch (catalog.type()) {
      case RELATIONAL:
        return RelationalCatalog.builder()
            .withNamespace(namespace)
            .withName(catalog.name())
            .withType(catalog.type())
            .withProvider(catalog.provider())
            .withComment(catalog.comment())
            .withProperties(catalog.properties())
            .withAudit((AuditDTO) catalog.auditInfo())
            .withRestClient(client)
            .build();

      case FILESET:
        return FilesetCatalog.builder()
            .withNamespace(namespace)
            .withName(catalog.name())
            .withType(catalog.type())
            .withProvider(catalog.provider())
            .withComment(catalog.comment())
            .withProperties(catalog.properties())
            .withAudit((AuditDTO) catalog.auditInfo())
            .withRestClient(client)
            .build();

      case MESSAGING:
        return MessagingCatalog.builder()
            .withNamespace(namespace)
            .withName(catalog.name())
            .withType(catalog.type())
            .withProvider(catalog.provider())
            .withComment(catalog.comment())
            .withProperties(catalog.properties())
            .withAudit((AuditDTO) catalog.auditInfo())
            .withRestClient(client)
            .build();
      default:
        throw new UnsupportedOperationException("Unsupported catalog type: " + catalog.type());
    }
  }

  static CatalogUpdateRequest toCatalogUpdateRequest(CatalogChange change) {
    if (change instanceof CatalogChange.RenameCatalog) {
      return new CatalogUpdateRequest.RenameCatalogRequest(
          ((CatalogChange.RenameCatalog) change).getNewName());

    } else if (change instanceof CatalogChange.UpdateCatalogComment) {
      return new CatalogUpdateRequest.UpdateCatalogCommentRequest(
          ((CatalogChange.UpdateCatalogComment) change).getNewComment());

    } else if (change instanceof CatalogChange.SetProperty) {
      return new CatalogUpdateRequest.SetCatalogPropertyRequest(
          ((CatalogChange.SetProperty) change).getProperty(),
          ((CatalogChange.SetProperty) change).getValue());

    } else if (change instanceof CatalogChange.RemoveProperty) {
      return new CatalogUpdateRequest.RemoveCatalogPropertyRequest(
          ((CatalogChange.RemoveProperty) change).getProperty());

    } else {
      throw new IllegalArgumentException(
          "Unknown change type: " + change.getClass().getSimpleName());
    }
  }

  static SchemaUpdateRequest toSchemaUpdateRequest(SchemaChange change) {
    if (change instanceof SchemaChange.SetProperty) {
      return new SchemaUpdateRequest.SetSchemaPropertyRequest(
          ((SchemaChange.SetProperty) change).getProperty(),
          ((SchemaChange.SetProperty) change).getValue());

    } else if (change instanceof SchemaChange.RemoveProperty) {
      return new SchemaUpdateRequest.RemoveSchemaPropertyRequest(
          ((SchemaChange.RemoveProperty) change).getProperty());

    } else {
      throw new IllegalArgumentException(
          "Unknown change type: " + change.getClass().getSimpleName());
    }
  }

  static TableUpdateRequest toTableUpdateRequest(TableChange change) {
    if (change instanceof TableChange.RenameTable) {
      return new TableUpdateRequest.RenameTableRequest(
          ((TableChange.RenameTable) change).getNewName());

    } else if (change instanceof TableChange.UpdateComment) {
      return new TableUpdateRequest.UpdateTableCommentRequest(
          ((TableChange.UpdateComment) change).getNewComment());

    } else if (change instanceof TableChange.SetProperty) {
      return new TableUpdateRequest.SetTablePropertyRequest(
          ((TableChange.SetProperty) change).getProperty(),
          ((TableChange.SetProperty) change).getValue());

    } else if (change instanceof TableChange.RemoveProperty) {
      return new TableUpdateRequest.RemoveTablePropertyRequest(
          ((TableChange.RemoveProperty) change).getProperty());

    } else if (change instanceof TableChange.ColumnChange) {
      return toColumnUpdateRequest((TableChange.ColumnChange) change);

    } else if (change instanceof TableChange.AddIndex) {
      return new TableUpdateRequest.AddTableIndexRequest(
          ((TableChange.AddIndex) change).getType(),
          ((TableChange.AddIndex) change).getName(),
          ((TableChange.AddIndex) change).getFieldNames());
    } else if (change instanceof TableChange.DeleteIndex) {
      return new TableUpdateRequest.DeleteTableIndexRequest(
          ((TableChange.DeleteIndex) change).getName(),
          ((TableChange.DeleteIndex) change).isIfExists());
    } else {
      throw new IllegalArgumentException(
          "Unknown change type: " + change.getClass().getSimpleName());
    }
  }

  static FilesetUpdateRequest toFilesetUpdateRequest(FilesetChange change) {
    if (change instanceof FilesetChange.RenameFileset) {
      return new FilesetUpdateRequest.RenameFilesetRequest(
          ((FilesetChange.RenameFileset) change).getNewName());
    } else if (change instanceof FilesetChange.UpdateFilesetComment) {
      return new FilesetUpdateRequest.UpdateFilesetCommentRequest(
          ((FilesetChange.UpdateFilesetComment) change).getNewComment());
    } else if (change instanceof FilesetChange.RemoveComment) {
      return new FilesetUpdateRequest.RemoveFilesetCommentRequest();
    } else if (change instanceof FilesetChange.SetProperty) {
      return new FilesetUpdateRequest.SetFilesetPropertiesRequest(
          ((FilesetChange.SetProperty) change).getProperty(),
          ((FilesetChange.SetProperty) change).getValue());
    } else if (change instanceof FilesetChange.RemoveProperty) {
      return new FilesetUpdateRequest.RemoveFilesetPropertiesRequest(
          ((FilesetChange.RemoveProperty) change).getProperty());
    } else {
      throw new IllegalArgumentException(
          "Unknown change type: " + change.getClass().getSimpleName());
    }
  }

  static TopicUpdateRequest toTopicUpdateRequest(TopicChange change) {
    if (change instanceof TopicChange.UpdateTopicComment) {
      return new TopicUpdateRequest.UpdateTopicCommentRequest(
          ((TopicChange.UpdateTopicComment) change).getNewComment());
    } else if (change instanceof TopicChange.SetProperty) {
      return new TopicUpdateRequest.SetTopicPropertyRequest(
          ((TopicChange.SetProperty) change).getProperty(),
          ((TopicChange.SetProperty) change).getValue());
    } else if (change instanceof TopicChange.RemoveProperty) {
      return new TopicUpdateRequest.RemoveTopicPropertyRequest(
          ((TopicChange.RemoveProperty) change).getProperty());
    } else {
      throw new IllegalArgumentException(
          "Unknown change type: " + change.getClass().getSimpleName());
    }
  }

  private static TableUpdateRequest toColumnUpdateRequest(TableChange.ColumnChange change) {
    if (change instanceof TableChange.AddColumn) {
      TableChange.AddColumn addColumn = (TableChange.AddColumn) change;
      Expression defaultValue;
      if (addColumn.getDefaultValue() == null
          || addColumn.getDefaultValue().equals(Column.DEFAULT_VALUE_NOT_SET)) {
        defaultValue = Column.DEFAULT_VALUE_NOT_SET;
      } else {
        defaultValue = toFunctionArg(addColumn.getDefaultValue());
      }
      return new TableUpdateRequest.AddTableColumnRequest(
          addColumn.fieldName(),
          addColumn.getDataType(),
          addColumn.getComment(),
          addColumn.getPosition(),
          addColumn.isNullable(),
          addColumn.isAutoIncrement(),
          defaultValue);

    } else if (change instanceof TableChange.RenameColumn) {
      TableChange.RenameColumn renameColumn = (TableChange.RenameColumn) change;
      return new TableUpdateRequest.RenameTableColumnRequest(
          renameColumn.fieldName(), renameColumn.getNewName());

    } else if (change instanceof TableChange.UpdateColumnDefaultValue) {
      return new TableUpdateRequest.UpdateTableColumnDefaultValueRequest(
          change.fieldName(),
          toFunctionArg(((TableChange.UpdateColumnDefaultValue) change).getNewDefaultValue()));

    } else if (change instanceof TableChange.UpdateColumnType) {
      return new TableUpdateRequest.UpdateTableColumnTypeRequest(
          change.fieldName(), ((TableChange.UpdateColumnType) change).getNewDataType());

    } else if (change instanceof TableChange.UpdateColumnComment) {
      return new TableUpdateRequest.UpdateTableColumnCommentRequest(
          change.fieldName(), ((TableChange.UpdateColumnComment) change).getNewComment());

    } else if (change instanceof TableChange.UpdateColumnPosition) {
      return new TableUpdateRequest.UpdateTableColumnPositionRequest(
          change.fieldName(), ((TableChange.UpdateColumnPosition) change).getPosition());

    } else if (change instanceof TableChange.DeleteColumn) {
      return new TableUpdateRequest.DeleteTableColumnRequest(
          change.fieldName(), ((TableChange.DeleteColumn) change).getIfExists());

    } else if (change instanceof TableChange.UpdateColumnNullability) {
      return new TableUpdateRequest.UpdateTableColumnNullabilityRequest(
          change.fieldName(), ((TableChange.UpdateColumnNullability) change).nullable());
    } else if (change instanceof TableChange.UpdateColumnAutoIncrement) {
      return new TableUpdateRequest.UpdateColumnAutoIncrementRequest(
          change.fieldName(), ((TableChange.UpdateColumnAutoIncrement) change).isAutoIncrement());
    } else {
      throw new IllegalArgumentException(
          "Unknown column change type: " + change.getClass().getSimpleName());
    }
  }

  static SecurableObjectDTO toSecurableObject(SecurableObject securableObject) {
    return SecurableObjectDTO.builder()
        .withFullName(securableObject.fullName())
        .withType(securableObject.type())
        .withPrivileges(
            securableObject.privileges().stream()
                .map(
                    privilege -> {
                      return PrivilegeDTO.builder()
                          .withCondition(privilege.condition())
                          .withName(privilege.name())
                          .build();
                    })
                .toArray(PrivilegeDTO[]::new))
        .build();
  }

  static TagUpdateRequest toTagUpdateRequest(TagChange change) {
    if (change instanceof TagChange.RenameTag) {
      return new TagUpdateRequest.RenameTagRequest(((TagChange.RenameTag) change).getNewName());

    } else if (change instanceof TagChange.UpdateTagComment) {
      return new TagUpdateRequest.UpdateTagCommentRequest(
          ((TagChange.UpdateTagComment) change).getNewComment());

    } else if (change instanceof TagChange.SetProperty) {
      return new TagUpdateRequest.SetTagPropertyRequest(
          ((TagChange.SetProperty) change).getProperty(),
          ((TagChange.SetProperty) change).getValue());

    } else if (change instanceof TagChange.RemoveProperty) {
      return new TagUpdateRequest.RemoveTagPropertyRequest(
          ((TagChange.RemoveProperty) change).getProperty());

    } else {
      throw new IllegalArgumentException(
          "Unknown change type: " + change.getClass().getSimpleName());
    }
  }
}
