/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.connect.jdbc.dialect;

import io.confluent.connect.jdbc.sink.metadata.SinkRecordField;
import io.confluent.connect.jdbc.util.ExpressionBuilder;
import io.confluent.connect.jdbc.util.TableId;
import org.apache.kafka.common.config.AbstractConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.List;

public class ClickHouseDatabaseDialect extends GenericDatabaseDialect {

  private static final Logger log = LoggerFactory.getLogger(ClickHouseDatabaseDialect.class);

  public static class Provider extends DatabaseDialectProvider.SubprotocolBasedProvider {
    public Provider() {
      super(ClickHouseDatabaseDialect.class.getSimpleName(), "clickhouse");
    }

    @Override
    public DatabaseDialect create(AbstractConfig config) {
      return new ClickHouseDatabaseDialect(config);
    }
  }

  // TODO: do we need to expand it to the correct one and not the defaults

  public ClickHouseDatabaseDialect(AbstractConfig config) {
    super(config);
  }

  @Override
  protected String getSqlType(SinkRecordField field) {
    switch (field.schemaType()) {
      case INT8:
        return "Int8";
      case INT16:
        return "Int16";
      case INT32:
        return "Int32";
      case INT64:
        return "Int64";
      case FLOAT32:
        return "Float32";
      case FLOAT64:
        return "Float64";
      case BOOLEAN:
        return "Bool";
      case STRING:
        return "String";
      case BYTES: // TODO: need to find the correct one
        return "varbinary(max)";
      default:
        return super.getSqlType(field);
    }
  }

  @Override
  public String buildCreateTableStatement(
          TableId table,
          Collection<SinkRecordField> fields
  ) {
    ExpressionBuilder builder = expressionBuilder();
    final List<String> pkFieldNames = extractPrimaryKeyFieldNames(fields);
    builder.append("CREATE TABLE ");
    builder.append(table);
    builder.append(" (");
    writeColumnsSpec(builder, fields);
    builder.append(") ");
    if (!pkFieldNames.isEmpty()) {
      builder.append("PRIMARY KEY(");
      builder.appendList()
              .delimitedBy(",")
              .transformedBy(ExpressionBuilder.quote())
              .of(pkFieldNames);
      builder.append(")");
    }
    final String createTableStatement = builder.toString();
    log.info("createTableStatement clickhouse: " + createTableStatement);
    return createTableStatement;
  }

}
