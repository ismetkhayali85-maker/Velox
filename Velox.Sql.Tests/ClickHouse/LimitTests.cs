#pragma warning disable CS0618

namespace Velox.Sql.Tests.ClickHouse;

public class LimitTests : TestBase
{
    [Fact]
    public void LimitOffset_ReturnsCorrectSql()
    {
        var sql = VeloxRuntime.ClickHouse<TestEntity>()
            .Limit(10)
            .Offset(5)
            .ToDebugSql();

        Assert.Equal("SELECT \"test_table\".\"id\" AS \"Id\", \"test_table\".\"name\" AS \"Name\" FROM \"test_table\" LIMIT 10 OFFSET 5;", sql);
    }
}
