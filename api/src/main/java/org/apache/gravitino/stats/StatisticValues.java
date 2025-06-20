/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.gravitino.stats;

import com.google.common.base.Preconditions;
import org.apache.gravitino.rel.types.Type;
import org.apache.gravitino.rel.types.Types;

import java.util.List;
import java.util.Map;

/**
 * A class representing a collection of statistic values.
 */
public class StatisticValues {

    /**
     * A statistic value that holds a Boolean value.
     */
    public static class BooleanValue implements StatisticValue<Boolean> {
        private final Boolean value;

        public BooleanValue(Boolean value) {
            this.value = value;
        }

        @Override
        public Boolean value() {
            return value;
        }

        @Override
        public Type dataType() {
            return Types.BooleanType.get();
        }
    }

    /**
     * A statistic value that holds a Long value.
     */
    public static class LongValue implements StatisticValue<Long> {
        private final Long value;

        public LongValue(Long value) {
            this.value = value;
        }

        @Override
        public Long value() {
            return value;
        }

        @Override
        public Type dataType() {
            return Types.LongType.get();
        }
    }

    /**
     * A statistic value that holds a Double value.
     */
    public static class DoubleValue implements StatisticValue<Double> {
        private final Double value;

        /**
         * Creates a DoubleValue with the given double value.
         * @param value The double value to be held by this statistic value.
         */
        public DoubleValue(Double value) {
            this.value = value;
        }

        @Override
        public Double value() {
            return value;
        }

        @Override
        public Type dataType() {
            return Types.DoubleType.get();
        }
    }

    /**
     * A statistic value that holds a String value.
     */
    public static class StringValue implements StatisticValue<String> {
        private final String value;

        /**
         * Creates a StringValue with the given string value.
         * @param value The string value to be held by this statistic value.
         */
        public StringValue(String value) {
            this.value = value;
        }

        @Override
        public String value() {
            return value;
        }

        @Override
        public Type dataType() {
            return Types.StringType.get();
        }
    }

    /**
     * A statistic value that holds a List of other statistic values.
     */
    public static class ListValue implements StatisticValue<List<StatisticValue>> {
        private final List<StatisticValue> values;

        /**
         * Creates a ListValue with the given list of statistic values.
         * @param values A list of StatisticValue instances.
         */
        public ListValue(List<StatisticValue> values) {
            Preconditions.checkArgument(values != null && values.isEmpty(), "Values cannot be null or empty");
            this.values = values;
        }

        @Override
        public List<StatisticValue> value() {
            return values;
        }

        @Override
        public Type dataType() {
            return Types.ListType.notNull(values.get(0).dataType());
        }
    }

    /**
     * A statistic value that holds a Map of String keys to other statistic values.
     */
    public static class ObjectValue implements StatisticValue<Map<String, StatisticValue>> {
        private final Map<String, StatisticValue> values;

        /**
         * Creates an ObjectValue with the given map of statistic values.
         * @param values A map where keys are String identifiers and values are StatisticValue instances.
         */
        public ObjectValue(Map<String, StatisticValue> values) {
            Preconditions.checkArgument(values != null && !values.isEmpty(), "Values cannot be null or empty");
            this.values = values;
        }

        @Override
        public Map<String, StatisticValue> value() {
            return values;
        }

        @Override
        public Type dataType() {
            return Types.StructType.of(
                    values.entrySet().stream()
                            .map(
                                    entry ->
                                            Types.StructType.Field.nullableField(
                                                    entry.getKey(), entry.getValue().dataType()))
                            .toArray(Types.StructType.Field[]::new));
        }
    }
}
