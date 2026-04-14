#pragma warning disable CS0618

namespace Velox.Sql.Tests.ClickHouse;

public class SortTests : TestBase
{
    [Fact]
    public void OrderBy_ReturnsCorrectSql()
    {
        var sql = VeloxRuntime.ClickHouse<TestEntity>()
            .OrderBy(true, x => x.Id)
            .ToDebugSql();

        Assert.Equal("SELECT \"test_table\".\"id\" AS \"Id\", \"test_table\".\"name\" AS \"Name\" FROM \"test_table\" ORDER BY \"test_table\".\"id\" ASC;", sql);
    }

    [Fact]
    public void OrderByAsc_ReturnsCorrectSql()
    {
        var sql = VeloxRuntime.ClickHouse<TestEntity>()
            .OrderByAsc(x => x.Id)
            .ToDebugSql();

        Assert.Equal("SELECT \"test_table\".\"id\" AS \"Id\", \"test_table\".\"name\" AS \"Name\" FROM \"test_table\" ORDER BY \"test_table\".\"id\" ASC;", sql);
    }

    [Fact]
    public void OrderByDesc_ReturnsCorrectSql()
    {
        var sql = VeloxRuntime.ClickHouse<TestEntity>()
            .OrderByDesc(x => x.Id)
            .ToDebugSql();

        Assert.Equal("SELECT \"test_table\".\"id\" AS \"Id\", \"test_table\".\"name\" AS \"Name\" FROM \"test_table\" ORDER BY \"test_table\".\"id\" DESC;", sql);
    }
}
