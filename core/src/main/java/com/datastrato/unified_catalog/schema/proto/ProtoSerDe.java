package com.datastrato.unified_catalog.schema.proto;

import com.google.protobuf.MessageLite;
import com.google.protobuf.Parser;

public interface ProtoSerDe<T, M extends MessageLite> {

  M serialize(T t);

  T deserialize(M p);

  default byte[] toBytes(T t) {
    return serialize(t).toByteArray();
  }

  default T fromBytes(byte[] bytes, Parser<M> parser) throws ProtoSerDeException {
    try {
      return deserialize(parser.parseFrom(bytes));
    } catch (Exception e) {
      throw new ProtoSerDeException("Failed in deserializing bytes to proto", e);
    }
  }
}
