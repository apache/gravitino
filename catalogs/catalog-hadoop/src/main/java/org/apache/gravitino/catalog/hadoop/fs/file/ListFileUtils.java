package org.apache.gravitino.catalog.hadoop.fs.file;

import java.io.IOException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.gravitino.json.JsonUtils;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/** @author libo */
public class ListFileUtils {

  private static final org.slf4j.Logger log =
      org.slf4j.LoggerFactory.getLogger(ListFileUtils.class);

  public static String buildTreeFile(FileSystem fileSystem, Path filesetPath) throws IOException {

    FileStatus[] fileStatuses = fileSystem.listStatus(filesetPath);
    List<FileInfo> fileInfos = new ArrayList<>();

    for (FileStatus fileStatus : fileStatuses) {

      FileInfo fileInfo = new FileInfo();
      String id = fileStatus.getPath().toString();
      fileInfo.setId(id);
      String[] charArray = id.split("/");
      fileInfo.setLabel(charArray[charArray.length - 1]);
      fileInfo.setPid("/");
      fileInfo.setFile(fileStatus.isFile());
      // 判断是否为文件？
      fileInfo.setLength(fileStatus.getLen() + "B");
      fileInfo.setModificationTime(date2String(fileStatus.getModificationTime()));
      fileInfo.setBlockReplication(fileStatus.getReplication() + "");
      fileInfo.setBlockSize(bytesToMegabytes(fileStatus.getBlockSize()) + "MB");

      fileInfos.add(fileInfo);
      log.info("fileInfo build:{}", JsonUtils.objectMapper().writeValueAsString(fileInfo));
      if (fileStatus.isDirectory()) {
        reverse(fileStatus, fileSystem, fileInfos);
      }
    }

    log.info("fileInfo all data:{}", JsonUtils.objectMapper().writeValueAsString(fileInfos));
    LabelVO labelVO = buildTree(fileInfos);

    String resultStr = JsonUtils.objectMapper().writeValueAsString(labelVO);
    log.info("listFile.return:{}", resultStr);
    return resultStr;
  }

  public static void reverse(FileStatus fileStatus, FileSystem fileSystem, List<FileInfo> fileInfos)
      throws IOException {
    if (!fileStatus.isDirectory()) {
      return;
    }
    Path pathTmp = new Path(fileStatus.getPath().toString());
    FileStatus[] fileStatuses = fileSystem.listStatus(pathTmp);

    for (FileStatus status : fileStatuses) {
      // 保存对象；
      FileInfo fileInfo = new FileInfo();
      String id = status.getPath().toString();
      fileInfo.setId(id);
      String[] charArray = id.split("/");
      fileInfo.setLabel(charArray[charArray.length - 1]);
      fileInfo.setPid(fileStatus.getPath().toString());

      fileInfo.setFile(fileStatus.isFile());
      fileInfo.setLength(fileStatus.getLen() + "B");
      fileInfo.setModificationTime(date2String(fileStatus.getModificationTime()));
      fileInfo.setBlockReplication(fileStatus.getReplication() + "");
      fileInfo.setBlockSize(bytesToMegabytes(fileStatus.getBlockSize()) + "MB");

      fileInfos.add(fileInfo);

      log.info("fileInfo reverse build:{}", JsonUtils.objectMapper().writeValueAsString(fileInfo));
      reverse(status, fileSystem, fileInfos);
    }
  }

  public static LabelVO buildTree(List<FileInfo> fileInfos) {
    Map<String, LabelVO> nodeMap = new HashMap<>();
    LabelVO root = new LabelVO(new FileInfo("/", "-1", "-1", false), "/", new ArrayList<>());
    nodeMap.put("/", root);

    for (FileInfo fileInfo : fileInfos) {
      LabelVO node = new LabelVO(fileInfo, fileInfo.getLabel(), new ArrayList<>());
      nodeMap.put(fileInfo.getId(), node);
    }

    for (FileInfo fileInfo : fileInfos) {

      LabelVO parentLabel = nodeMap.get(fileInfo.getPid());
      if (parentLabel != null) {
        parentLabel.getChildren().add(nodeMap.get(fileInfo.getId()));
      }
    }
    return root;
  }

  private static String date2String(long timestamp) {

    Instant instant = Instant.ofEpochMilli(timestamp);
    // 将 Instant 对象转换为 LocalDateTime 对象，使用系统默认时区
    LocalDateTime localDateTime = LocalDateTime.ofInstant(instant, ZoneId.systemDefault());
    // 定义日期格式
    DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm");
    return localDateTime.format(formatter);
  }

  public static double bytesToMegabytes(long bytes) {
    return (double) bytes / (1024 * 1024);
  }
}
