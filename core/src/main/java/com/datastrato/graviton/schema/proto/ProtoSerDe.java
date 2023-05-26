package com.datastrato.graviton.schema.proto;

import com.google.protobuf.MessageLite;

public interface ProtoSerDe<T, M extends MessageLite> {

  M serialize(T t);

  T deserialize(M p);
}
