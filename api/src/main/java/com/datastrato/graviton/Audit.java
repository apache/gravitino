package com.datastrato.graviton;

import java.time.Instant;

public interface Audit {

  String creator();

  Instant createTime();

  String lastModifier();

  Instant lastModifiedTime();
}
