package org.apache.gravitino.catalog.hadoop.fs.file;

import java.io.Serializable;

public class FileInfo implements Serializable {
  private static final long serialVersionUID = -8056997047303822111L;

  /** 当前文件或文件夹名 */
  private String label;

  /** 唯一标识 */
  private String id;
  /** 上一级目录id */
  private String pid;

  /** 是否为文件 */
  private Boolean file;

  /** 文件大小，单位byte */
  private String length;
  /** 上次更新时间 */
  private String modificationTime;
  /** 副本数量 */
  private String blockReplication;
  /** 分块大小 */
  private String blockSize;

  public FileInfo() {}

  public FileInfo(String label, String id, String pid, Boolean file) {
    this.label = label;
    this.id = id;
    this.pid = pid;
    this.file = file;
  }

  public FileInfo(
      String label,
      String id,
      String pid,
      Boolean file,
      String length,
      String modificationTime,
      String blockReplication,
      String blockSize) {
    this.label = label;
    this.id = id;
    this.pid = pid;
    this.file = file;
    this.length = length;
    this.modificationTime = modificationTime;
    this.blockReplication = blockReplication;
    this.blockSize = blockSize;
  }

  public String getLabel() {
    return label;
  }

  public void setLabel(String label) {
    this.label = label;
  }

  public String getId() {
    return id;
  }

  public void setId(String id) {
    this.id = id;
  }

  public String getPid() {
    return pid;
  }

  public void setPid(String pid) {
    this.pid = pid;
  }

  public Boolean getFile() {
    return file;
  }

  public void setFile(Boolean file) {
    this.file = file;
  }

  public String getLength() {
    return length;
  }

  public void setLength(String length) {
    this.length = length;
  }

  public String getModificationTime() {
    return modificationTime;
  }

  public void setModificationTime(String modificationTime) {
    this.modificationTime = modificationTime;
  }

  public String getBlockReplication() {
    return blockReplication;
  }

  public void setBlockReplication(String blockReplication) {
    this.blockReplication = blockReplication;
  }

  public String getBlockSize() {
    return blockSize;
  }

  public void setBlockSize(String blockSize) {
    this.blockSize = blockSize;
  }
}
