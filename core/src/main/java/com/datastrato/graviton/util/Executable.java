package com.datastrato.graviton.util;

@FunctionalInterface
public interface Executable<T> {

  T execute() throws Exception;
}
