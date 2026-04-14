#pragma warning disable CS0618

namespace Velox.Sql.Tests.Postgres;

public class WindowTests : TestBase
{
    [Fact]
    public void SumOver_PartitionAndOrder_ReturnsCorrectSql()
    {
        var sql = VeloxRuntime.Postgres<PostgresTestEntity>()
            .Select(x => x.Description)
            .SumOver<PostgresTestEntity>(x => x.Id,
                w => w.PartitionBy<PostgresTestEntity>(x => x.Description)
                    .OrderByAsc<PostgresTestEntity>(x => x.Id),
                "running")
            .ToDebugSql();

        Assert.Equal(
            "SELECT \"pg_table\".\"description\" AS \"Description\", SUM(\"pg_table\".\"id\") OVER (PARTITION BY \"pg_table\".\"description\" ORDER BY \"pg_table\".\"id\" ASC) AS \"running\" FROM \"pg_table\";",
            sql);
    }

    [Fact]
    public void RowNumberOver_PartitionOnly_ReturnsCorrectSql()
    {
        var sql = VeloxRuntime.Postgres<PostgresTestEntity>()
            .Select(x => x.Id)
            .RowNumberOver(w => w.PartitionBy<PostgresTestEntity>(x => x.Description), "rn")
            .ToDebugSql();

        Assert.Equal(
            "SELECT \"pg_table\".\"id\" AS \"Id\", ROW_NUMBER() OVER (PARTITION BY \"pg_table\".\"description\") AS \"rn\" FROM \"pg_table\";",
            sql);
    }

    [Fact]
    public void CountOver_EmptyWindow_ReturnsCorrectSql()
    {
        var sql = VeloxRuntime.Postgres<PostgresTestEntity>()
            .Select(x => x.Id)
            .CountOver(_ => { }, "c")
            .ToDebugSql();

        Assert.Equal(
            "SELECT \"pg_table\".\"id\" AS \"Id\", COUNT(*) OVER () AS \"c\" FROM \"pg_table\";",
            sql);
    }

    [Fact]
    public void AvgOver_OrderByDesc_ReturnsCorrectSql()
    {
        var sql = VeloxRuntime.Postgres<PostgresTestEntity>()
            .AvgOver<PostgresTestEntity>(x => x.Id,
                w => w.OrderByDesc<PostgresTestEntity>(x => x.Description),
                "avg_id")
            .ToDebugSql();

        Assert.Equal(
            "SELECT AVG(\"pg_table\".\"id\") OVER (ORDER BY \"pg_table\".\"description\" DESC) AS \"avg_id\" FROM \"pg_table\";",
            sql);
    }
}
