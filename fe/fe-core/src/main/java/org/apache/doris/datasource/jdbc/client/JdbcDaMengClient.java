// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.doris.datasource.jdbc.client;

import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.Type;
import org.apache.doris.datasource.jdbc.util.JdbcFieldSchema;

import com.google.common.collect.ImmutableSet;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Set;

public class JdbcDaMengClient extends JdbcClient {
    private static final Logger LOG = LogManager.getLogger(JdbcDaMengClient.class);

    protected JdbcDaMengClient(JdbcClientConfig jdbcClientConfig) {
        super(jdbcClientConfig);
    }

    @Override
    public String getTestQuery() {
        return "SELECT 1 FROM dual";
    }

    @Override
    protected Set<String> getFilterInternalDatabases() {
        return ImmutableSet.<String>builder()
                .add("ctxsys")
                .add("flows_files")
                .add("mdsys")
                .add("outln")
                .add("sys")
                .add("system")
                .add("xdb")
                .add("xs$null")
                .build();
    }

    @Override
    protected Type jdbcTypeToDoris(JdbcFieldSchema fieldSchema) {
        String daMengType = fieldSchema.getDataTypeName().orElse("unknown");
        if (daMengType.startsWith("INTERVAL")) {
            daMengType = daMengType.substring(0, 8);
        } else if (daMengType.startsWith("TIMESTAMP") || daMengType.equals("DATETIME")) {
            if (daMengType.contains("TIME ZONE") || daMengType.contains("LOCAL TIME ZONE")) {
                return Type.UNSUPPORTED;
            }
            // oracle can support nanosecond, will lose precision
            int scale = fieldSchema.getDecimalDigits().orElse(0);
            if (scale > 6) {
                scale = 6;
            }
            return ScalarType.createDatetimeV2Type(scale);
        }
        switch (daMengType) {
            case "NUMBER":
            case "NUMERIC":
            case "DECIMAL":
            case "DEC":
                int precision = fieldSchema.getColumnSize().orElse(0);
                int scale = fieldSchema.getDecimalDigits().orElse(0);
                /**
                 * type is NUMERIC, rather than NUMERIC(a, b)
                 * that means scale = 38 and precision unlimited, use string
                 * */
                if (scale == 0 && precision == 0) {
                    return ScalarType.createStringType();
                }

                if (scale <= 0) {
                    precision -= scale;
                    if (precision < 3) {
                        return Type.TINYINT;
                    } else if (precision < 5) {
                        return Type.SMALLINT;
                    } else if (precision < 10) {
                        return Type.INT;
                    } else if (precision < 19) {
                        return Type.BIGINT;
                    } else if (precision < 39) {
                        // LARGEINT supports up to 38 numbers.
                        return Type.LARGEINT;
                    } else {
                        return ScalarType.createStringType();
                    }
                }
                // scale > 0
                if (precision < scale) {
                    precision = scale;
                }
                return createDecimalOrStringType(precision, scale);
            case "INT":
            case "INTEGER":
                return Type.INT;
            case "BIGINT":
                return Type.BIGINT;
            case "TINYINT":
                return Type.TINYINT;
            case "SMALLINT":
                return Type.SMALLINT;
            case "BIT":
            case "BOOLEAN":
            case "BOOL":
                return Type.BOOLEAN;
            case "FLOAT":
                return Type.FLOAT;
            case "DOUBLE":
                return Type.DOUBLE;
            case "DATE":
                // can save date and time with second precision
                return ScalarType.createDateV2Type();
            case "VARCHAR":
            case "VARCHAR2":
            case "TEXT":
            case "LONGVARCHAR":
            case "CHAR":
            case "CHARACTER":
            case "NCHAR":
            case "LONG":
            case "RAW":
            case "LONG RAW":
            case "INTERVAL":
            case "BYTE":
            case "TIME":
            case "CLOB":
                return ScalarType.createStringType();
            case "BLOB":
            case "NCLOB":
            case "IMAGE":
            case "LONGVARBINARY":
            case "BFILE":
            case "BINARY_FLOAT":
            case "BINARY_DOUBLE":
            default:
                return Type.UNSUPPORTED;
        }
    }
}
