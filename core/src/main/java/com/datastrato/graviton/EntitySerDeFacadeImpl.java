/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.graviton;

import com.datastrato.graviton.meta.BaseMetalake;
import com.datastrato.graviton.meta.CatalogEntity;
import com.datastrato.graviton.util.Bytes;
import com.datastrato.graviton.util.IsolatedClassLoader;
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;
import org.apache.commons.lang3.tuple.Pair;

public class EntitySerDeFacadeImpl implements EntitySerDeFacade {
  private static final BiMap<String, Integer> ENTITY_TO_PROTO_ID = HashBiMap.create();

  static {
    ENTITY_TO_PROTO_ID.put(BaseMetalake.class.getCanonicalName(), 0);
    ENTITY_TO_PROTO_ID.put(CatalogEntity.class.getCanonicalName(), 1);
    ENTITY_TO_PROTO_ID.put(com.datastrato.graviton.meta.AuditInfo.class.getCanonicalName(), 2);
  }

  /**
   * Length of Header added to the serialized byte array object, more information please refer to
   * {@link #serializeClassInfo(Class, byte[])}
   */
  public static final int HEADER_LEN = 20;

  private EntitySerDe entitySerDe;
  /**
   * This method will also header information like a class type to a byte array that is to say it
   * contains the class type and the object content
   *
   * <pre>The final content will be:
   * --------------------------
   * |Header | object content |
   * --------------------------
   *
   * And the `Header` will be(total 20 bytes, See {@link #HEADER_LEN})
   * ---------------------------
   * | class type | reserverd  |
   * ---------------------------
   *     4 byte      16 byte
   * </pre>
   */
  private byte[] serializeClassInfo(Class<?> clazz, byte[] objectContent) {
    Integer index = ENTITY_TO_PROTO_ID.get(clazz.getCanonicalName());
    if (Objects.isNull(index)) {
      throw new IllegalArgumentException(
          "The class " + clazz.getCanonicalName() + " is not registered");
    }

    byte[] header = new byte[HEADER_LEN];
    ByteBuffer headerBuffer = ByteBuffer.wrap(header);
    headerBuffer.putInt(index);
    return Bytes.concat(header, objectContent);
  }

  /**
   * This method will try to get the class name from the byte array and then load the class from the
   * class loader and then get object content
   *
   * <p>For more please refer to {@link #serializeClassInfo(Class, byte[])}
   */
  private Pair<byte[], Class<? extends Entity>> getObjectContentAndClass(
      byte[] classInfoAndContent, ClassLoader loader) throws IOException {
    ByteBuffer byteBuffer = ByteBuffer.wrap(classInfoAndContent);
    int index = byteBuffer.getInt();
    String className = ENTITY_TO_PROTO_ID.inverse().get(index);
    Class<? extends Entity> clazz =
        (Class<? extends Entity>) IsolatedClassLoader.loadClass(className, loader);
    byte[] contents = new byte[classInfoAndContent.length - HEADER_LEN];
    System.arraycopy(classInfoAndContent, HEADER_LEN, contents, 0, contents.length);

    return Pair.of(contents, clazz);
  }

  @Override
  public <T extends Entity> byte[] serialize(T t) throws IOException {
    byte[] objectContent = entitySerDe.serialize(t);
    return serializeClassInfo(t.getClass(), objectContent);
  }

  @Override
  public <T extends Entity> T deserialize(byte[] bytes, ClassLoader classLoader)
      throws IOException {
    Pair<byte[], Class<? extends Entity>> contentsAndType =
        getObjectContentAndClass(bytes, classLoader);
    byte[] contents = contentsAndType.getLeft();
    Class<? extends Entity> clazz = contentsAndType.getRight();

    return (T) entitySerDe.deserialize(contents, clazz);
  }

  @Override
  public void setEntitySeDe(EntitySerDe serDe) {
    this.entitySerDe = serDe;
  }
}
