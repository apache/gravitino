package org.apache.gravitino.cli.commands;

import org.apache.gravitino.Audit;

public abstract class AuditCommand extends Command {
  /**
   * @param url The URL of the Gravitino server.
   * @param ignoreVersions If true don't check the client/server versions match.
   */
  public AuditCommand(String url, boolean ignoreVersions) {
    super(url, ignoreVersions);
  }

  /* Overridden in parent - do nothing  */
  @Override
  public void handle() {}

  /**
   * Displays audit information for the given audit object.
   *
   * @param audit from a class that implements the Auditable interface.
   */
  public void displayAuditInfo(Audit audit) {
    String auditInfo =
        "creator,createTime,lastModifier,lastModifiedTime"
            + System.lineSeparator()
            + audit.creator()
            + ","
            + audit.createTime()
            + ","
            + audit.lastModifier()
            + ","
            + audit.lastModifiedTime();

    System.out.println(auditInfo);
  }
}
