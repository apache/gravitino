package org.apache.gravitino.storage.relational.utils;

public class TestEntity2 {
  private Long id;
  private String name;

  // 构造方法
  public TestEntity2() {}

  public TestEntity2(Long id, String name) {
    this.id = id;
    this.name = name;
  }

  // Getter 和 Setter 方法
  public Long getId() {
    return id;
  }

  public void setId(Long id) {
    this.id = id;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  @Override
  public String toString() {
    return "TestEntity1{" + "id=" + id + ", name='" + name + '\'' + '}';
  }
}
