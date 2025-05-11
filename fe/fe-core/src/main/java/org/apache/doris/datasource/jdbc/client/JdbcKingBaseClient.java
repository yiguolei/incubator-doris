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

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;

public class JdbcKingBaseClient extends JdbcClient {

    protected JdbcKingBaseClient(JdbcClientConfig jdbcClientConfig) {
        super(jdbcClientConfig);
        Connection conn = null;
        Statement stmt = null;
        ResultSet rs = null;
        try {
            conn = super.getConnection();
            stmt = conn.createStatement();
            rs = stmt.executeQuery("show database_mode");
            if (rs.next()) {
                String compatibilityMode = rs.getString(1);
                if (!"ORACLE".equalsIgnoreCase(compatibilityMode)) {
                    throw new JdbcClientException("Only KingBase Oracle compatibility mode is supported, but got: "
                            + compatibilityMode);
                }
            } else {
                throw new JdbcClientException("Failed to determine KingBase compatibility mode");
            }
        } catch (SQLException | JdbcClientException e) {
            closeClient();
            throw new JdbcClientException("Failed to initialize JdbcMySQLClient: %s", e.getMessage());
        } finally {
            close(rs, stmt, conn);
        }
    }

    @Override
    public String getTestQuery() {
        return "SELECT 1 FROM dual";
    }

    @Override
    protected Type jdbcTypeToDoris(JdbcFieldSchema fieldSchema) {
        switch (fieldSchema.getDataType()) {
            case Types.BIT:
                return Type.BOOLEAN;
            case Types.TINYINT:
                return Type.TINYINT;
            case Types.SMALLINT:
                return Type.SMALLINT;
            case Types.INTEGER:
                return Type.INT;
            case Types.BIGINT:
                return Type.BIGINT;
            case Types.FLOAT:
            case Types.REAL:
                return Type.FLOAT;
            case Types.DOUBLE:
                return Type.DOUBLE;
            case Types.NUMERIC:
            case Types.DECIMAL: {
                int precision = fieldSchema.getColumnSize()
                        .orElseThrow(() -> new IllegalArgumentException("Precision not present"));
                int scale = fieldSchema.getDecimalDigits()
                        .orElseThrow(() -> new JdbcClientException("Scale not present"));
                return createDecimalOrStringType(precision, scale);
            }
            case Types.DATE:
                return Type.DATEV2;
            case Types.TIMESTAMP: {
                int scale = fieldSchema.getDecimalDigits().orElse(0);
                if (scale > 6) {
                    scale = 6;
                }
                return ScalarType.createDatetimeV2Type(scale);
            }
            case Types.CHAR:
                return ScalarType.createCharType(fieldSchema.requiredColumnSize());
            case Types.TIME:
            case Types.VARCHAR:
            case Types.LONGVARCHAR:
            case Types.CLOB:
            case Types.NCLOB:
            case Types.OTHER:
                return ScalarType.createStringType();
            default:
                return Type.UNSUPPORTED;
        }
    }
}
