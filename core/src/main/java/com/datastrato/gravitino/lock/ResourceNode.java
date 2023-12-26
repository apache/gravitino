/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.lock;

import com.datastrato.gravitino.NameIdentifier;
import com.google.common.base.Objects;
import com.google.common.collect.Maps;
import java.util.Map;
import javax.annotation.concurrent.ThreadSafe;

@ThreadSafe
public class ResourceNode implements Resource<ResourceNode> {

  private NameIdentifier identifier;

  private ResourceNode parent;

  private final Map<NameIdentifier, ResourceNode> child;

  public ResourceNode() {
    this.child = Maps.newConcurrentMap();
  }

  @Override
  public ResourceNode parent() {
    return parent;
  }

  @Override
  public NameIdentifier resourceIdentifier() {
    return identifier;
  }

  @Override
  public void addChild(ResourceNode resourceNode) {
    child.put(resourceNode.resourceIdentifier(), resourceNode);
  }

  @Override
  public ResourceNode getChild(NameIdentifier identifier) {
    return child.get(identifier);
  }

  @Override
  public void setParent(ResourceNode resourceNode) {
    this.parent = resourceNode;
  }

  @Override
  public void setIdentifier(NameIdentifier identifier) {
    this.identifier = identifier;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ResourceNode that = (ResourceNode) o;
    return Objects.equal(identifier, that.identifier);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(identifier);
  }
}
