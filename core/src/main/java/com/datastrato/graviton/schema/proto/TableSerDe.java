package com.datastrato.graviton.schema.proto;

import com.datastrato.graviton.schema.HasExtraInfo;
import com.datastrato.graviton.schema.VirtualTableInfo;

class TableSerDe implements ProtoSerDe<com.datastrato.graviton.schema.Table, Table> {

  @Override
  public Table serialize(com.datastrato.graviton.schema.Table table) {
    Table.Builder builder = Table.newBuilder();
    builder
        .setId(table.getId())
        .setZoneId(table.getZoneId())
        .setName(table.getName())
        .setType(Table.TableType.valueOf(table.getType().getTypeStr()))
        .setSnapshotId(table.getSnapshotId())
        .setAuditInfo(new AuditInfoSerDe().serialize(table.auditInfo()));

    if (table.getComment() != null) {
      builder.setComment(table.getComment());
    }

    if (table.getProperties() != null && !table.getProperties().isEmpty()) {
      builder.putAllProperties(table.getProperties());
    }

    setTableExtraInfo(builder, table.getType(), table.getExtraInfo());

    return builder.build();
  }

  @Override
  public com.datastrato.graviton.schema.Table deserialize(Table p) {
    com.datastrato.graviton.schema.Table.Builder builder =
        new com.datastrato.graviton.schema.Table.Builder();
    builder
        .withId(p.getId())
        .withZoneId(p.getZoneId())
        .withName(p.getName())
        .withType(com.datastrato.graviton.schema.Table.TableType.valueOf(p.getType().name()))
        .withSnapshotId(p.getSnapshotId())
        .withAuditInfo(new AuditInfoSerDe().deserialize(p.getAuditInfo()));

    if (p.hasComment()) {
      builder.withComment(p.getComment());
    }

    if (p.getPropertiesCount() > 0) {
      builder.withProperties(p.getPropertiesMap());
    }

    builder.withExtraInfo(
        getTableExtraInfo(
            com.datastrato.graviton.schema.Table.TableType.valueOf(p.getType().name()), p));

    return builder.build();
  }

  private void setTableExtraInfo(
      Table.Builder builder,
      com.datastrato.graviton.schema.Table.TableType tableType,
      HasExtraInfo.ExtraInfo tableExtraInfo) {
    switch (tableType) {
      case VIRTUAL:
        VirtualTableInfo virtualTableInfo = (VirtualTableInfo) tableExtraInfo;
        builder.setVirtualTable(
            Table.VirtualTableInfo.newBuilder()
                .setConnectionId(virtualTableInfo.getConnectionId())
                .addAllTableIdentifier(virtualTableInfo.getIdentifier())
                .build());
        break;

      case VIEW:
      case EXTERNAL:
      case MANAGED:
        // TODO. Add support for these types.
        throw new ProtoSerDeException("Table type " + tableType + " is not supported yet.");

      default:
        throw new ProtoSerDeException("Unknown table type " + tableType + ".");
    }
  }

  private HasExtraInfo.ExtraInfo getTableExtraInfo(
      com.datastrato.graviton.schema.Table.TableType tableType, Table p) {
    switch (tableType) {
      case VIRTUAL:
        if (!p.hasVirtualTable()) {
          throw new ProtoSerDeException("Virtual table info is missing.");
        }

        return new VirtualTableInfo(
            p.getVirtualTable().getConnectionId(), p.getVirtualTable().getTableIdentifierList());

      case VIEW:
      case EXTERNAL:
      case MANAGED:
        // TODO. Add support for these types.
        throw new ProtoSerDeException("Table type " + tableType + " is not supported yet.");

      default:
        throw new ProtoSerDeException("Unknown table type " + tableType + ".");
    }
  }
}
