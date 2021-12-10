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
package io.trino.plugin.kudu;

import io.trino.sql.planner.plan.LimitNode;
import io.trino.testing.BaseConnectorTest;
import io.trino.testing.MaterializedResult;
import io.trino.testing.QueryRunner;
import io.trino.testing.TestingConnectorBehavior;
import io.trino.testing.sql.TestTable;
import org.testng.SkipException;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.util.Optional;
import java.util.regex.Pattern;

import static io.trino.plugin.kudu.KuduQueryRunnerFactory.createKuduQueryRunnerTpch;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.testing.MaterializedResult.resultBuilder;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_DELETE;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_TOPN_PUSHDOWN;
import static io.trino.testing.assertions.Assert.assertEquals;
import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.Assert.assertTrue;

public abstract class BaseKuduConnectorTest
        extends BaseConnectorTest
{
    private TestingKuduServer kuduServer;

    protected abstract Optional<String> getKuduSchemaEmulationPrefix();

    @Override
    protected boolean hasBehavior(TestingConnectorBehavior connectorBehavior)
    {
        switch (connectorBehavior) {
            case SUPPORTS_ARRAY:
            case SUPPORTS_COMMENT_ON_TABLE:
            case SUPPORTS_COMMENT_ON_COLUMN:
            case SUPPORTS_RENAME_SCHEMA:
            case SUPPORTS_TOPN_PUSHDOWN:
                return false;
            case SUPPORTS_DELETE:
                return true;
            default:
                return super.hasBehavior(connectorBehavior);
        }
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        kuduServer = new TestingKuduServer();
        return createKuduQueryRunnerTpch(kuduServer, getKuduSchemaEmulationPrefix(), REQUIRED_TPCH_TABLES);
    }

    @AfterClass(alwaysRun = true)
    public final void destroy()
    {
        kuduServer.close();
    }

    @Override
    protected TestTable createTableWithDefaultColumns()
    {
        throw new SkipException("Kudu connector does not support column default values");
    }

    @Override
    public void testInsert()
    {
        // TODO Support these test once kudu connector can create tables with default partitions
    }

    @Override
    public void testCommentTable()
    {
        // TODO
        throw new SkipException("TODO");
    }

    @Override
    public void testCommentColumn()
    {
        // TODO
        throw new SkipException("TODO");
    }

    @Override
    public void testAddColumn()
    {
        // TODO Support these test once kudu connector can create tables with default partitions
        throw new SkipException("TODO");
    }

    @Override
    public void testCreateTable()
    {
        // TODO Support these test once kudu connector can create tables with default partitions
        throw new SkipException("TODO");
    }

    @Override
    public void testInsertUnicode()
    {
        // TODO Support these test once kudu connector can create tables with default partitions
        throw new SkipException("TODO");
    }

    @Override
    public void testInsertHighestUnicodeCharacter()
    {
        // TODO Support these test once kudu connector can create tables with default partitions
        throw new SkipException("TODO");
    }

    @Override
    public void testDropNonEmptySchema()
    {
        // TODO Support these test once kudu connector can create tables with default partitions
        throw new SkipException("TODO");
    }

    @Override
    public void testDelete()
    {
        // TODO Support these test once kudu connector can create tables with default partitions
        throw new SkipException("TODO");
    }

    @Override
    protected void skipTestUnlessSupportsDeletes()
    {
        // TODO Remove override once kudu connector can create tables with default partitions
        if (hasBehavior(SUPPORTS_DELETE)) {
            assertQueryFails("DELETE FROM region", "This connector does not support deletes");
            throw new SkipException("This connector does not support deletes");
        }
    }

    @Override
    public void testShowColumns()
    {
        MaterializedResult actual = computeActual("SHOW COLUMNS FROM orders");

        MaterializedResult expectedParametrizedVarchar = resultBuilder(getSession(), VARCHAR, VARCHAR, VARCHAR, VARCHAR)
                .row("orderkey", "bigint", "nullable, encoding=auto, compression=default", "")
                .row("custkey", "bigint", "nullable, encoding=auto, compression=default", "")
                .row("orderstatus", "varchar", "nullable, encoding=auto, compression=default", "")
                .row("totalprice", "double", "nullable, encoding=auto, compression=default", "")
                .row("orderdate", "varchar", "nullable, encoding=auto, compression=default", "")
                .row("orderpriority", "varchar", "nullable, encoding=auto, compression=default", "")
                .row("clerk", "varchar", "nullable, encoding=auto, compression=default", "")
                .row("shippriority", "integer", "nullable, encoding=auto, compression=default", "")
                .row("comment", "varchar", "nullable, encoding=auto, compression=default", "")
                .build();

        assertEquals(actual, expectedParametrizedVarchar);
    }

    @Override
    @Test
    public void testWrittenStats()
    {
        // TODO Kudu connector supports CTAS and inserts, but the test would fail
        throw new SkipException("TODO");
    }

    @Override
    public void testColumnName(String columnName)
    {
        // TODO (https://github.com/trinodb/trino/issues/3477) enable the test
        throw new SkipException("TODO");
    }

    @Override
    protected Optional<DataMappingTestSetup> filterDataMappingSmokeTestData(DataMappingTestSetup dataMappingTestSetup)
    {
        String typeName = dataMappingTestSetup.getTrinoTypeName();
        if (typeName.equals("time")
                || typeName.equals("timestamp(3) with time zone")) {
            return Optional.of(dataMappingTestSetup.asUnsupported());
        }

        if (typeName.equals("date") // date gets stored as varchar
                || typeName.equals("varbinary") // TODO (https://github.com/trinodb/trino/issues/3416)
                || (typeName.startsWith("char") && dataMappingTestSetup.getSampleValueLiteral().contains(" "))) { // TODO: https://github.com/trinodb/trino/issues/3597
            // TODO this should either work or fail cleanly
            return Optional.empty();
        }

        return Optional.of(dataMappingTestSetup);
    }

    @Test
    @Override
    public void testDescribeTable()
    {
        String extra = "nullable, encoding=auto, compression=default";
        MaterializedResult expectedColumns = resultBuilder(getSession(), VARCHAR, VARCHAR, VARCHAR, VARCHAR)
                .row("orderkey", "bigint", extra, "")
                .row("custkey", "bigint", extra, "")
                .row("orderstatus", "varchar", extra, "")
                .row("totalprice", "double", extra, "")
                .row("orderdate", "varchar", extra, "")
                .row("orderpriority", "varchar", extra, "")
                .row("clerk", "varchar", extra, "")
                .row("shippriority", "integer", extra, "")
                .row("comment", "varchar", extra, "")
                .build();
        MaterializedResult actualColumns = computeActual("DESCRIBE orders");
        assertEquals(actualColumns, expectedColumns);
    }

    @Test
    @Override
    public void testShowCreateTable()
    {
        assertThat((String) computeActual("SHOW CREATE TABLE orders").getOnlyValue())
                .matches("CREATE TABLE kudu\\.\\w+\\.orders \\Q(\n" +
                        "   orderkey bigint WITH ( nullable = true ),\n" +
                        "   custkey bigint WITH ( nullable = true ),\n" +
                        "   orderstatus varchar WITH ( nullable = true ),\n" +
                        "   totalprice double WITH ( nullable = true ),\n" +
                        "   orderdate varchar WITH ( nullable = true ),\n" +
                        "   orderpriority varchar WITH ( nullable = true ),\n" +
                        "   clerk varchar WITH ( nullable = true ),\n" +
                        "   shippriority integer WITH ( nullable = true ),\n" +
                        "   comment varchar WITH ( nullable = true )\n" +
                        ")\n" +
                        "WITH (\n" +
                        "   number_of_replicas = 3,\n" +
                        "   partition_by_hash_buckets = 2,\n" +
                        "   partition_by_hash_columns = ARRAY['row_uuid'],\n" +
                        "   partition_by_range_columns = ARRAY['row_uuid'],\n" +
                        "   range_partitions = '[{\"lower\":null,\"upper\":null}]'\n" +
                        ")");

        assertUpdate("CREATE TABLE IF NOT EXISTS test_show_create_table (\n" +
                "id INT WITH (primary_key=true),\n" +
                "user_name VARCHAR\n" +
                ") WITH (\n" +
                " partition_by_hash_columns = ARRAY['id'],\n" +
                " partition_by_hash_buckets = 2," +
                " number_of_replicas = 1\n" +
                ")");

        MaterializedResult result = computeActual("SHOW CREATE TABLE test_show_create_table");
        String sqlStatement = (String) result.getOnlyValue();
        String tableProperties = sqlStatement.split("\\)\\s*WITH\\s*\\(")[1];
        assertTableProperty(tableProperties, "number_of_replicas", "1");
        assertTableProperty(tableProperties, "partition_by_hash_columns", Pattern.quote("ARRAY['id']"));
        assertTableProperty(tableProperties, "partition_by_hash_buckets", "2");

        assertUpdate("DROP TABLE test_show_create_table");
    }

    @Test
    public void testRowDelete()
    {
        assertUpdate("CREATE TABLE IF NOT EXISTS test_row_delete (" +
                "id INT WITH (primary_key=true), " +
                "second_id INT, " +
                "user_name VARCHAR" +
                ") WITH (" +
                " partition_by_hash_columns = ARRAY['id'], " +
                " partition_by_hash_buckets = 2" +
                ")");

        assertUpdate("INSERT INTO test_row_delete VALUES (0, 1, 'user0'), (3, 4, 'user2'), (2, 3, 'user2'), (1, 2, 'user1')", 4);
        assertQuery("SELECT count(*) FROM test_row_delete", "VALUES 4");

        assertUpdate("DELETE FROM test_row_delete WHERE second_id = 4", 1);
        assertQuery("SELECT count(*) FROM test_row_delete", "VALUES 3");

        assertUpdate("DELETE FROM test_row_delete WHERE user_name = 'user1'", 1);
        assertQuery("SELECT count(*) FROM test_row_delete", "VALUES 2");

        assertUpdate("DELETE FROM test_row_delete WHERE id = 0", 1);
        assertQuery("SELECT * FROM test_row_delete", "VALUES (2, 3, 'user2')");

        assertUpdate("DROP TABLE test_row_delete");
    }

    @Test
    public void testProjection()
    {
        assertUpdate("CREATE TABLE IF NOT EXISTS test_projection (" +
                "id INT WITH (primary_key=true), " +
                "user_name VARCHAR " +
                ") WITH (" +
                " partition_by_hash_columns = ARRAY['id'], " +
                " partition_by_hash_buckets = 2" +
                ")");

        assertUpdate("INSERT INTO test_projection VALUES (0, 'user0'), (2, 'user2'), (1, 'user1')", 3);

        assertQuery("SELECT id, 'test' FROM test_projection ORDER BY id", "VALUES (0, 'test'), (1, 'test'), (2, 'test')");

        assertUpdate("DROP TABLE test_projection");
    }

    @Test
    public void testLimitPushdown()
    {
        assertThat(query("SELECT name FROM nation LIMIT 30")).isNotFullyPushedDown(LimitNode.class); // Use high limit for result determinism
    }

    private void assertTableProperty(String tableProperties, String key, String regexValue)
    {
        assertTrue(Pattern.compile(key + "\\s*=\\s*" + regexValue + ",?\\s+").matcher(tableProperties).find(),
                "Not found: " + key + " = " + regexValue + " in " + tableProperties);
    }
}
