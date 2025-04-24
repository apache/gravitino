---
title: Event reference 
slug: /event-reference
license: "This software is licensed under the Apache License version 2."
---

## Events

Gravitino triggers a pre-event before an operation, a post-event after the completion of that operation.
It also triggers a failure event if the operation failed.

### Pre-event

<table>
<thead>
<tr>
  <th>Operation type</th>
  <th>Pre-event</th>
  <th>Since version</th>
</tr>
</thead>
<tbody>
<tr>
  <td>Iceberg REST server table operation</td>
  <td>
    `IcebergCreateTablePreEvent`<br/>
    `IcebergDropTablePreEvent`<br/>
    `IcebergListTablePreEvent`<br/>
    `IcebergLoadTablePreEvent`<br/>
    `IcebergRenameTablePreEvent`<br/>
    `IcebergTableExistsPreEvent`<br/>
    `IcebergUpdateTablePreEvent`
  </td>
  <td>0.7.0-incubating</td>
</tr>
<tr>
  <td>Gravitino server table operation</td>
  <td>
    `AlterTablePreEvent`<br/>
    `CreateTablePreEvent`<br/>
    `DropTablePreEvent`<br/>
    `ListTablePreEvent`<br/>
    `LoadTablePreEvent`<br/>
    `PurgeTablePreEvent`
  </td>
  <td>0.8.0-incubating</td>
</tr>
<tr>
  <td>Gravitino server schema operation</td>
  <td>
    `AlterSchemaPreEvent`<br/>
    `CreateSchemaPreEvent`<br/>
    `DropSchemaPreEvent`<br/>
    `ListSchemaPreEvent`<br/>
    `LoadSchemaPreEvent`
  </td>
  <td>0.8.0-incubating</td>
</tr>
<tr>
  <td>Gravitino server catalog operation</td>
  <td>
    `AlterCatalogPreEvent`<br/>
    `CreateCatalogPreEvent`<br/>
    `DropCatalogPreEvent`<br/>
    `ListCatalogPreEvent`<br/>
    `LoadCatalogPreEvent`
  </td>
  <td>0.8.0-incubating</td>
</tr>
<tr>
  <td>Gravitino server metalake operation</td>
  <td>
    `AlterMetalakePreEvent`<br/>
    `CreateMetalakePreEvent`<br/>
    `DropMetalakePreEvent`<br/>
    `ListMetalakePreEvent`<br/>
    `LoadMetalakePreEvent`
  </td>
  <td>0.8.0-incubating</td>
</tr>
<tr>
  <td>Gravitino server partition operation</td>
  <td>
    `AddPartitionPreEvent`<br/>
    `DropPartitionPreEvent`<br/>
    `GetPartitionPreEvent`<br/>
    `PurgePartitionPreEvent`<br/>
    `ListPartitionNamesPreEvent`<br/>
    `ListPartitionPreEvent`
  </td>
  <td>0.8.0-incubating</td>
</tr>
<tr>
  <td>Gravitino server fileset operation</td>
  <td>
    `AlterFilesetPreEvent`<br/>
    `CreateFilesetPreEvent`<br/>
    `DropFilesetPreEvent`<br/>
    `GetFileLocationPreEvent`<br/>
    `ListFilesetPreEvent`<br/>
    `LoadFilesetPreEvent`
  </td>
  <td>0.8.0-incubating</td>
</tr>
<tr>
  <td>Gravitino server model operation</td>
  <td>
    `DeleteModelPreEvent`<br/>
    `DeleteModelVersionPreEvent`<br/>
    `GetModelPreEvent`<br/>
    `GetModelVersionPreEvent`<br/>
    `LinkModelVersionPreEvent`<br/>
    `ListModelPreEvent`<br/>
    `RegisterAndLinkModelPreEvent`<br/>
    `RegisterModelPreEvent`
  </td>
  <td>0.9.0-incubating</td>
</tr>
<tr>
  <td>Gravitino server tag operation</td>
  <td>
    `AssociateTagsForMetadataObjectPreEvent`<br/>
    `AlterTagPreEvent`<br/>
    `CreateTagPreEvent`<br/>
    `DeleteTagPreEvent`<br/>
    `GetTagForMetadataObjectPreEvent`<br/>
    `GetTagPreEvent`<br/>
    `ListMetadataObjectsForTagPreEvent`<br/>
    `ListTagsForMetadataObjectPreEvent`<br/>
    `ListTagsInfoPreEvent`<br/>
    `ListTagsInfoForMetadataObjectPreEvent`<br/>
    `ListTagsPreEvent`
  </td>
  <td>0.9.0-incubating</td>
</tr>
<tr>
  <td>Gravitino server user operation</td>
  <td>
    `AddUserPreEvent`<br/>
    `GetUserPreEvent`<br/>
    `GrantUserRolesPreEvent`<br/>
    `ListUserNamesPreEvent`<br/>
    `ListUsersPreEvent`<br/>
    `RemoveUserPreEvent`<br/>
    `RevokeUserRolesPreEvent`
  </td>
  <td>0.9.0-incubating</td>
</tr>
<tr>
  <td>Gravitino server group operation</td>
  <td>
    `AddGroupPreEvent`<br/>
    `GetGroupPreEvent`<br/>
    `GrantGroupRolesPreEvent`<br/>
    `ListGroupNamesPreEvent`<br/>
    `ListGroupsPreEvent`<br/>
    `RemoveGroupPreEvent`<br/>
    `RevokeGroupRolesPreEvent`
  </td>
  <td>0.9.0-incubating</td>
</tr>
<tr>
  <td>Gravitino server role operation</td>
  <td>
    `CreateRolePreEvent`<br/>
    `DeleteRolePreEvent`<br/>
    `GetRolePreEvent`<br/>
    `GrantPrivilegesPreEvent`<br/>
    `ListRoleNamesPreEvent`<br/>
    `RevokePrivilegesPreEvent`
  </td>
  <td>0.9.0-incubating</td>
</tr>

</tbody>
</table>

### Post-events

<table>
<thead>
<tr>
  <th>Operation type</th>
  <th>Post-event</th>
  <th>Since version</th>
</tr>
</thead>
<tbody>
<tr>
  <td>table operation</td>
  <td>
    `AlterTableEvent`<br/>
    `AlterTableFailureEvent`<br/>
    `CreateTableEvent`<br/>
    `CreateTableFailureEvent`<br/>
    `DropTableEvent`<br/>
    `DropTableFailureEvent`<br/>
    `ListTableEvent`<br/>
    `ListTableFailureEvent`<br/>
    `LoadTableEvent`<br/>
    `LoadTableFailureEvent`<br/>
    `PurgeTableEvent`<br/>
    `PurgeTableFailureEvent`
  </td>
  <td>0.5.0</td>
</tr>
<tr>
  <td>fileset operation</td>
  <td>
    `AlterFileSetEvent`<br/>
    `AlterFileSetFailureEvent`<br/>
    `CreateFileSetEvent`<br/>
    `CreateFileSetFailureEvent`<br/>
    `DropFileSetEvent`<br/>
    `DropFileSetFailureEvent`<br/>
    `ListFileSetEvent`<br/>
    `ListFileSetFailureEvent`<br/>
    `LoadFileSetEvent`<br/>
    `LoadFileSetFailureEvent`
  </td>
  <td>0.5.0</td>
</tr>
<tr>
  <td>topic operation</td>
  <td>
    `AlterTopicEvent`<br/>
    `AlterTopicFailureEvent`<br/>
    `CreateTopicEvent`<br/>
    `CreateTopicFailureEvent`<br/>
    `DropTopicEvent`<br/>
    `DropTopicFailureEvent`<br/>
    `ListTopicEvent`<br/>
    `ListTopicFailureEvent`<br/>
    `LoadTopicEvent`<br/>
    `LoadTopicFailureEvent`
  </td>
  <td>0.5.0</td>
</tr>
<tr>
  <td>schema operation</td>
  <td>
    `AlterSchemaEvent`<br/>
    `AlterSchemaFailureEvent`<br/>
    `CreateSchemaEvent`<br/>
    `CreateSchemaFailureEvent`<br/>
    `DropSchemaEvent`<br/>
    `DropSchemaFailureEvent`<br/>
    `ListSchemaEvent`<br/>
    `ListSchemaFailureEvent`<br/>
    `LoadSchemaEvent`<br/>
    `LoadSchemaFailureEvent`
  </td>
  <td>0.5.0</td>
</tr>
<tr>
  <td>catalog operation</td>
  <td>
    `AlterCatalogEvent`<br/>
    `AlterCatalogFailureEvent`<br/>
    `CreateCatalogEvent`<br/>
    `CreateCatalogFailureEvent`<br/>
    `DropCatalogEvent`<br/>
    `DropCatalogFailureEvent`<br/>
    `ListCatalogEvent`<br/>
    `ListCatalogFailureEvent`<br/>
    `LoadCatalogEvent`<br/>
    `LoadCatalogFailureEvent`
  </td>
  <td>0.5.0</td>
</tr>
<tr>
  <td>metalake operation</td>
  <td>
    `AlterMetalakeEvent`<br/>
    `AlterMetalakeFailureEvent`<br/>
    `CreateMetalakeEvent`<br/>
    `CreateMetalakeFailureEvent`<br/>
    `DropMetalakeEvent`<br/>
    `DropMetalakeFailureEvent`<br/>
    `ListMetalakeEvent`<br/>
    `ListMetalakeFailureEvent`<br/>
    `LoadMetalakeEvent`<br/>
    `LoadMetalakeFailureEvent`
  </td>
  <td>0.5.0</td>
</tr>
<tr>
  <td>Iceberg REST server table operation</td>
  <td>
    `IcebergCreateTableEvent`<br/>
    `IcebergCreateTableFailureEvent`<br/>
    `IcebergDropTableEvent`<br/>
    `IcebergDropTableFailureEvent`<br/>
    `IcebergListTableEvent`<br/>
    `IcebergListTableFailureEvent`<br/>
    `IcebergLoadTableEvent`<br/>
    `IcebergLoadTableFailureEvent`<br/>
    `IcebergRenameTableEvent`<br/>
    `IcebergRenameTableFailureEvent`<br/>
    `IcebergTableExistsEvent`<br/>
    `IcebergTableExistsFailureEvent`<br/>
    `IcebergUpdateTableEvent`<br/>
    `IcebergUpdateTableFailureEvent`
  </td>
  <td>0.7.0-incubating</td>
</tr>
<tr>
  <td>tag operation</td>
  <td>
    `AlterTagEvent`<br/>
    `AlterTagFailureEvent`<br/>
    `AssociateTagsForMetadataObjectEvent`<br/>
    `AssociateTagsForMetadataObjectFailureEvent`<br/>
    `CreateTagEvent`<br/>
    `CreateTagFailureEvent`<br/>
    `DeleteTagEvent`<br/>
    `DeleteTagFailureEvent`<br/>
    `GetTagEvent`<br/>
    `GetTagFailureEvent`<br/>
    `GetTagForMetadataObjectEvent`<br/>
    `GetTagForMetadataObjectFailureEvent`<br/>
    `ListMetadataObjectsForTagEvent`<br/>
    `ListMetadataObjectsForTagFailureEvent`<br/>
    `ListTagsEvent`<br/>
    `ListTagsFailureEvent`<br/>
    `ListTagsForMetadataObjectEvent`<br/>
    `ListTagsForMetadataObjectFailureEvent`<br/>
    `ListTagsInfoEvent`<br/>
    `ListTagsInfoFailureEvent`<br/>
    `ListTagsInfoForMetadataObjectEvent`<br/>
    `ListTagsInfoForMetadataObjectFailureEvent`
  </td>
  <td>0.9.0-incubating</td>
</tr>
<tr>
  <td>model operation</td>
  <td>
    `DeleteModelEvent`<br/>
    `DeleteModelFailureEvent`<br/>
    `DeleteModelVersionEvent`<br/>
    `DeleteModelVersionFailureEvent`<br/>
    `GetModelEvent`<br/>
    `GetModelFailureEvent`<br/>
    `GetModelVersionEvent`<br/>
    `GetModelVersionFailureEvent`<br/>
    `LinkModelVersionEvent`<br/>
    `LinkModelVersionFailureEvent`<br/>
    `ListModelEvent`<br/>
    `ListModelFailureEvent`<br/>
    `ListModelVersionsEvent`<br/>
    `ListModelVersionFailureEvent`<br/>
    `RegisterAndLinkModelEvent`<br/>
    `RegisterAndLinkModelFailureEvent`<br/>
    `RegisterModelEvent`<br/>
    `RegisterModelFailureEvent`
  </td>
  <td>0.9.0-incubating</td>
</tr>
<tr>
  <td>user operation</td>
  <td>
    `AddUserEvent`<br/>
    `AddUserFailureEvent`<br/>
    `GetUserEvent`<br/>
    `GetUserFailureEvent`<br/>
    `GrantUserRolesEvent`<br/>
    `GrantUserRolesFailureEvent`<br/>
    `ListUserNamesEvent`<br/>
    `ListUserNamesFailureEvent`<br/>
    `ListUsersEvent`<br/>
    `ListUsersFailureEvent`<br/>
    `RemoveUserEvent`<br/>
    `RemoveUserFailureEvent`<br/>
    `RevokeUserRolesEvent`<br/>
    `RevokeUserRolesFailureEvent`
  </td>
  <td>0.9.0-incubating</td>
</tr>
<tr>
  <td>group operation</td>
  <td>
    `AddGroupEvent`<br/>
    `AddGroupFailureEvent`<br/>
    `GetGroupEvent`<br/>
    `GetGroupFailureEvent`<br/>
    `GrantGroupRolesEvent`<br/>
    `GrantGroupRolesFailureEvent`<br/>
    `ListGroupNamesEvent`<br/>
    `ListGroupNamesFailureEvent`<br/>
    `ListGroupsEvent`<br/>
    `ListGroupsFailureEvent`<br/>
    `RemoveGroupEvent`<br/>
    `RemoveGroupFailureEvent`<br/>
    `RevokeGroupRolesEvent`<br/>
    `RevokeGroupRolesFailureEvent`
  </td>
  <td>0.9.0-incubating</td>
</tr>
</tbody>
</table>

