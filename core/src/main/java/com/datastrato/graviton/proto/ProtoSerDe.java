package com.datastrato.graviton.proto;

import com.google.protobuf.Message;

public interface ProtoSerDe<T, M extends Message> {

  M serialize(T t);

  T deserialize(M p);
}
