/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.connector.write;

import java.io.Serializable;
import java.util.Iterator;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.catalog.functions.AggregateFunction;
import org.apache.spark.sql.connector.expressions.NamedReference;
import org.apache.spark.sql.types.StructType;

public interface RequiresAggregateFiltering extends RowLevelOperation {
  FilterDefinition getFilterDefinition(FilterType filterType);

  interface FilterDefinition {
    NamedReference[] groupingKey();
    NamedReference[] inputAttributes();
    AggregateFunction<?, ?> aggregateFunction();
    OutputWriter outputWriter();
  }

  interface OutputWriter extends Serializable {
    Iterator<InternalRow> write(Iterator<InternalRow> aggRows);
    StructType outputType();
  }

  enum FilterType {
    MATCHED, MATCHED_BY_SOURCE, NOT_MATCHED_BY_SOURCE
  }
}
