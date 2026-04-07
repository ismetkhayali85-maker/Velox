#pragma warning disable CS0618

namespace Velox.Sql.Tests.ClickHouse;

public class GroupByTests : TestBase
{
    [Fact]
    public void GroupBy_ReturnsCorrectSql()
    {
        var sql = DbQuery<TestEntity>.GetClickHouseBuilder()
            .Select()
            .GroupBy(x => x.Name)
            .ToDebugSql();

        Assert.Equal("SELECT \"test_table\".\"id\" AS \"Id\", \"test_table\".\"name\" AS \"Name\" FROM \"test_table\" GROUP BY \"test_table\".\"name\";", sql);
    }
}
