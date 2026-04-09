#pragma warning disable CS0618

namespace Velox.Sql.Tests.ClickHouse;

public class InsertTests : TestBase
{
    [Fact]
    public void Insert_ReturnsCorrectSql()
    {
        var entity = new TestEntity { Id = 1, Name = "test" };
        var sql = VeloxRuntime.ClickHouse<TestEntity>()
            .Insert(entity)
            .ToDebugSql();

        Assert.Equal("INSERT INTO \"test_table\" (\"id\",\"name\") VALUES (1, 'test');", sql);
    }

    [Fact]
    public void BulkInsert_ReturnsCorrectSql()
    {
        var data = new[] { new TestEntity { Id = 1, Name = "A" }, new TestEntity { Id = 2, Name = "B" } };
        var builder = VeloxRuntime.ClickHouse<TestEntity>();
        var insertBuilder = builder.BulkInsert(data);

        AssertQuery(insertBuilder,
            debug: "INSERT INTO \"test_table\" (\"id\",\"name\") VALUES (1, 'A'),(2, 'B');",
            sql:   "INSERT INTO \"test_table\" (\"id\",\"name\") VALUES (1, 'A'),(2, 'B');",
            expectedParams: new { });
    }

    [Fact]
    public void Insert_Nullable_IncludesNull_ReturnsCorrectSql()
    {
        var entity = new NullableTestEntity { Id = 1, Name = null };
        var sql = VeloxRuntime.ClickHouse<NullableTestEntity>()
            .Insert(entity)
            .ToDebugSql();

        Assert.Equal("INSERT INTO \"nullable_test_table\" (\"id\",\"name\") VALUES (1, NULL);", sql);
    }


}
