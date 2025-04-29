package org.apache.gravitino.catalog.hadoop.fs.file;

import java.io.Serializable;
import java.util.List;

public class LabelVO implements Serializable {

  private static final long serialVersionUID = -8056997047303822111L;
  private FileInfo fileInfo;
  private String label;
  private List<LabelVO> children;

  public LabelVO() {}

  public LabelVO(FileInfo fileInfo, String label, List<LabelVO> children) {
    this.fileInfo = fileInfo;
    this.label = label;
    this.children = children;
  }

  public FileInfo getFileInfo() {
    return fileInfo;
  }

  public void setFileInfo(FileInfo fileInfo) {
    this.fileInfo = fileInfo;
  }

  public String getLabel() {
    return label;
  }

  public void setLabel(String label) {
    this.label = label;
  }

  public List<LabelVO> getChildren() {
    return children;
  }

  public void setChildren(List<LabelVO> children) {
    this.children = children;
  }
}
