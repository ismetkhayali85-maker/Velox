using Velox.Sql;
using Velox.Sql.Impl;
using Velox.Sql.Impl.Map;
using Velox.Sql.Interfaces;

namespace Velox.Sql.Tests;

public class TestEntity
{
    public int Id { get; set; }
    public string Name { get; set; }
}

public class DateTimeEntity
{
    public int Id { get; set; }
    public DateTime CreatedAt { get; set; }
}

public class NullableTestEntity
{
    public int Id { get; set; }
    public string? Name { get; set; }
}

public class PostgresTestEntity
{
    public int Id { get; set; }
    public string Description { get; set; }
}

public class PostgresNullableTestEntity
{
    public int Id { get; set; }
    public string Description { get; set; }
}

public class PostgresJoinEntity
{
    public int Id { get; set; }
    public string Name { get; set; }
    public int ParentId { get; set; }
}

public class ClickHouseJoinEntity
{
    public int Id { get; set; }
    public string Name { get; set; }
    public int ParentId { get; set; }
}

public class TestEntityMapper : Mapper<TestEntity>
{
    public TestEntityMapper()
    {
        Table("test_table");
        Map(x => x.Id).Column("id");
        Map(x => x.Name).Column("name");
        Build();
    }
}

public class DateTimeEntityMapper : Mapper<DateTimeEntity>
{
    public DateTimeEntityMapper()
    {
        Table("date_table");
        Map(x => x.Id).Column("id");
        Map(x => x.CreatedAt).Column("created_at");
        Build();
    }
}

public class NullableTestEntityMapper : Mapper<NullableTestEntity>
{
    public NullableTestEntityMapper()
    {
        Table("nullable_test_table");
        Map(x => x.Id).Column("id");
        Map(x => x.Name).Column("name").Nullable();
        Build();
    }
}

public class PostgresTestEntityMapper : Mapper<PostgresTestEntity>
{
    public PostgresTestEntityMapper()
    {
        Table("pg_table");
        Map(x => x.Id).Column("id");
        Map(x => x.Description).Column("description");
        Build();
    }
}

public class PostgresNullableTestEntityMapper : Mapper<PostgresNullableTestEntity>
{
    public PostgresNullableTestEntityMapper()
    {
        Table("pg_nullable_table");
        Map(x => x.Id).Column("id");
        Map(x => x.Description).Column("description").Nullable();
        Build();
    }
}

public class PostgresJoinEntityMapper : Mapper<PostgresJoinEntity>
{
    public PostgresJoinEntityMapper()
    {
        Table("pg_join_table");
        Map(x => x.Id).Column("id");
        Map(x => x.Name).Column("name");
        Map(x => x.ParentId).Column("parent_id");
        Build();
    }
}

public class ClickHouseJoinEntityMapper : Mapper<ClickHouseJoinEntity>
{
    public ClickHouseJoinEntityMapper()
    {
        Table("ch_join_table");
        Map(x => x.Id).Column("id");
        Map(x => x.Name).Column("name");
        Map(x => x.ParentId).Column("parent_id");
        Build();
    }
}

public abstract class TestBase
{
    static TestBase()
    {
        // Global Postgres Config - Register all test entities
        DbQuery.DefaultPostgresConfig = new PgSqlConfiguration(new List<IClassMapper> 
        { 
            new PostgresTestEntityMapper(),
            new DateTimeEntityMapper(),
            new PostgresNullableTestEntityMapper(),
            new PostgresJoinEntityMapper()
        });

        // Global ClickHouse Config
        DbQuery.DefaultClickHouseConfig = new ClickHouseSqlConfiguration(new List<IClassMapper> 
        { 
            new TestEntityMapper(),
            new DateTimeEntityMapper(),
            new NullableTestEntityMapper(),
            new ClickHouseJoinEntityMapper()
        });
    }

    protected void AssertQuery(ISqlBuilder builder, string debug = null, string sql = null, object expectedParams = null)
    {
        // 1. Check Debug SQL
        if (debug != null)
        {
            var debugSql = builder.ToDebugSql();
            Xunit.Assert.Equal(debug, debugSql);
        }

        // 2. Check Parameterized SQL
        if (sql != null)
        {
            var query = builder.ToSql();
            Xunit.Assert.Equal(sql, query.Sql);

            if (expectedParams != null)
            {
                var expectedDict = new Dictionary<string, object>();
                if (expectedParams is Dictionary<string, object> dict)
                {
                    expectedDict = dict;
                }
                else
                {
                    // Convert anonymous object to dictionary
                    foreach (var prop in expectedParams.GetType().GetProperties())
                        expectedDict.Add(prop.Name, prop.GetValue(expectedParams));
                }

                Xunit.Assert.Equal(expectedDict.Count, query.Parameters.Count);
                foreach (var kvp in expectedDict)
                {
                    Xunit.Assert.True(query.Parameters.ContainsKey(kvp.Key), $"Parameter {kvp.Key} not found.");
                    Xunit.Assert.Equal(kvp.Value, query.Parameters[kvp.Key]);
                }
            }
        }
    }
}
