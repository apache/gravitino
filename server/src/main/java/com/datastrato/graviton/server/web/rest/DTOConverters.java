package com.datastrato.graviton.server.web.rest;

import com.datastrato.graviton.Audit;
import com.datastrato.graviton.Lakehouse;
import com.datastrato.graviton.dto.AuditDTO;
import com.datastrato.graviton.dto.LakehouseDTO;

public class DTOConverters {

  private DTOConverters() {}

  public static AuditDTO toDTO(Audit audit) {
    return AuditDTO.builder()
        .withCreator(audit.creator())
        .withCreateTime(audit.createTime())
        .withLastModifier(audit.lastModifier())
        .withLastModifiedTime(audit.lastModifiedTime())
        .build();
  }

  public static LakehouseDTO toDTO(Lakehouse lakehouse) {
    return LakehouseDTO.builder()
        .withName(lakehouse.name())
        .withComment(lakehouse.comment())
        .withProperties(lakehouse.properties())
        .withAudit(toDTO(lakehouse.auditInfo()))
        .build();
  }
}
