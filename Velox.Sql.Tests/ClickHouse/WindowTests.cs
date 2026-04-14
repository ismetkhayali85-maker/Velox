#pragma warning disable CS0618

namespace Velox.Sql.Tests.ClickHouse;

public class WindowTests : TestBase
{
    [Fact]
    public void SumOver_PartitionAndOrder_ReturnsCorrectSql()
    {
        var sql = VeloxRuntime.ClickHouse<TestEntity>()
            .Select(x => x.Name)
            .SumOver<TestEntity>(x => x.Id,
                w => w.PartitionBy<TestEntity>(x => x.Name)
                    .OrderByAsc<TestEntity>(x => x.Id),
                "running")
            .ToDebugSql();

        Assert.Equal(
            "SELECT \"test_table\".\"name\" AS \"Name\", sum(\"test_table\".\"id\") OVER (PARTITION BY \"test_table\".\"name\" ORDER BY \"test_table\".\"id\" ASC) AS \"running\" FROM \"test_table\";",
            sql);
    }

    [Fact]
    public void RowNumberOver_PartitionOnly_ReturnsCorrectSql()
    {
        var sql = VeloxRuntime.ClickHouse<TestEntity>()
            .Select(x => x.Id)
            .RowNumberOver(w => w.PartitionBy<TestEntity>(x => x.Name), "rn")
            .ToDebugSql();

        Assert.Equal(
            "SELECT \"test_table\".\"id\" AS \"Id\", row_number() OVER (PARTITION BY \"test_table\".\"name\") AS \"rn\" FROM \"test_table\";",
            sql);
    }

    [Fact]
    public void CountOver_EmptyWindow_ReturnsCorrectSql()
    {
        var sql = VeloxRuntime.ClickHouse<TestEntity>()
            .Select(x => x.Id)
            .CountOver(_ => { }, "c")
            .ToDebugSql();

        Assert.Equal(
            "SELECT \"test_table\".\"id\" AS \"Id\", COUNT(*) OVER () AS \"c\" FROM \"test_table\";",
            sql);
    }

    [Fact]
    public void AvgOver_OrderByDesc_ReturnsCorrectSql()
    {
        var sql = VeloxRuntime.ClickHouse<TestEntity>()
            .AvgOver<TestEntity>(x => x.Id,
                w => w.OrderByDesc<TestEntity>(x => x.Name),
                "avg_id")
            .ToDebugSql();

        Assert.Equal(
            "SELECT avg(\"test_table\".\"id\") OVER (ORDER BY \"test_table\".\"name\" DESC) AS \"avg_id\" FROM \"test_table\";",
            sql);
    }
}
