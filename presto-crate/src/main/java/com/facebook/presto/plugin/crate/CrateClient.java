/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.plugin.crate;

import com.facebook.presto.plugin.jdbc.BaseJdbcClient;
import com.facebook.presto.plugin.jdbc.BaseJdbcConfig;
import com.facebook.presto.plugin.jdbc.JdbcColumnHandle;
import com.facebook.presto.plugin.jdbc.JdbcConnectorId;
import com.facebook.presto.plugin.jdbc.JdbcTableHandle;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.crate.client.jdbc.CrateDriver;

import javax.inject.Inject;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static com.facebook.presto.plugin.jdbc.JdbcErrorCode.JDBC_ERROR;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static java.util.Locale.ENGLISH;

public class CrateClient
        extends BaseJdbcClient
{
    @Inject
    public CrateClient(JdbcConnectorId connectorId, BaseJdbcConfig config, TypeManager typeManager)
            throws SQLException
    {
        super(connectorId, config, "", new CrateDriver());
    }

    @Override
    public Set<String> getSchemaNames()
    {
        try {
            Connection connection = driver.connect(connectionUrl, connectionProperties);
            Statement schemasStatement = connection.createStatement();
            ResultSet resultSet = schemasStatement.executeQuery("SELECT schema_name FROM information_schema.schemata");
            ImmutableSet.Builder<String> schemaNames = ImmutableSet.builder();
            while (resultSet.next()) {
                String schemaName = resultSet.getString("schema_name").toLowerCase(ENGLISH);
                // skip internal schemas
                if (!schemaName.equals("information_schema")) {
                    schemaNames.add(schemaName);
                }
            }
            return schemaNames.build();
        }
        catch (SQLException e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    protected ResultSet getTables(Connection connection, String schemaName, String tableName)
            throws SQLException
    {
        try {
            PreparedStatement preparedStatement;
            if (tableName == null) {
                preparedStatement = connection.prepareStatement(
                        "SELECT schema_name as \"TABLE_CAT\", table_name as \"TABLE_NAME\", NULL as \"TABLE_SCHEM\" " +
                                "FROM information_schema.tables WHERE schema_name = ? ");
                preparedStatement.setString(1, schemaName);
            }
            else {
                preparedStatement = connection.prepareStatement(
                        "SELECT schema_name as \"TABLE_CAT\", table_name as \"TABLE_NAME\", NULL as \"TABLE_SCHEM\" " +
                                "FROM information_schema.tables WHERE schema_name = ? AND table_name = ? ");
                preparedStatement.setString(1, schemaName);
                preparedStatement.setString(2, tableName);
            }

            ResultSet resultSet = preparedStatement.executeQuery();
            return resultSet;
        }
        catch (SQLException e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    protected SchemaTableName getSchemaTableName(ResultSet resultSet)
            throws SQLException
    {
        // MySQL uses catalogs instead of schemas
        return new SchemaTableName(
                resultSet.getString("TABLE_CAT").toLowerCase(ENGLISH),
                resultSet.getString("TABLE_NAME").toLowerCase(ENGLISH));
    }

    @Override
    public List<JdbcColumnHandle> getColumns(JdbcTableHandle tableHandle)
    {
        String schemaName = tableHandle.getCatalogName();
        String tableName = tableHandle.getTableName();

        List<JdbcColumnHandle> columns = new ArrayList<>();

        try (Connection connection = driver.connect(connectionUrl, connectionProperties)) {
            PreparedStatement preparedStatement = connection.prepareStatement(
                    "SELECT column_name, data_type " +
                            "FROM information_schema.columns  WHERE schema_name = ? AND table_name = ? ");
            preparedStatement.setString(1, schemaName);
            preparedStatement.setString(2, tableName);

            ResultSet resultSet = preparedStatement.executeQuery();
            while (resultSet.next()) {
                String columnName = resultSet.getString("column_name");
                String crateType = resultSet.getString("data_type");
                Type columnType = toPrestoType(crateType);
                columns.add(new JdbcColumnHandle(connectorId, columnName, columnType));
            }

            return ImmutableList.copyOf(columns);
        }
        catch (SQLException e) {
            throw new PrestoException(JDBC_ERROR, e);
        }
    }

    protected Type toPrestoType(String type)
    {
        switch (type) {
            case "boolean":
                return BOOLEAN;
            case "integer":
            case "long":
            case "timestamp":
                return BIGINT;
            case "float":
            case "double":
                return DOUBLE;
        }
        return VARCHAR;
    }
}
