package com.datastrato.graviton.util;

@FunctionalInterface
public interface ThrowableFunction<T, R> {

  R apply(T t) throws Exception;
}
