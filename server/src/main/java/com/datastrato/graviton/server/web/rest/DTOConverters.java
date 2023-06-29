package com.datastrato.graviton.server.web.rest;

import com.datastrato.graviton.Audit;
import com.datastrato.graviton.Metalake;
import com.datastrato.graviton.dto.AuditDTO;
import com.datastrato.graviton.dto.MetalakeDTO;

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

  public static MetalakeDTO toDTO(Metalake metalake) {
    return MetalakeDTO.builder()
        .withName(metalake.name())
        .withComment(metalake.comment())
        .withProperties(metalake.properties())
        .withAudit(toDTO(metalake.auditInfo()))
        .build();
  }
}
