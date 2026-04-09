#pragma warning disable CS0618
namespace Velox.Sql.Tests.ClickHouse;

public class JoinTests : TestBase
{
    [Fact]
    public void InnerJoin_ReturnsCorrectSql()
    {
        var sql = VeloxRuntime.ClickHouse<TestEntity>()
            .Select(x => x.Id)
            .InnerJoin<ClickHouseJoinEntity, TestEntity>(
                j => j.ParentId,
                p => p.Id,
                (j, p) => new { j.Name })
            .ToDebugSql();

        Assert.Equal("SELECT \"test_table\".\"id\" AS \"Id\", \"ch_join_table\".\"name\" AS \"Name\" FROM \"test_table\" INNER JOIN \"ch_join_table\" ON \"ch_join_table\".\"parent_id\" = \"test_table\".\"id\";", sql);
    }

    [Fact]
    public void LeftJoin_ReturnsCorrectSql()
    {
        var sql = VeloxRuntime.ClickHouse<TestEntity>()
            .Select(x => x.Id)
            .LeftJoin<ClickHouseJoinEntity, TestEntity>(
                j => j.ParentId,
                p => p.Id)
            .ToDebugSql();

        Assert.Equal("SELECT \"test_table\".\"id\" AS \"Id\" FROM \"test_table\" LEFT JOIN \"ch_join_table\" ON \"ch_join_table\".\"parent_id\" = \"test_table\".\"id\";", sql);
    }

    [Fact]
    public void RightJoin_ReturnsCorrectSql()
    {
        var sql = VeloxRuntime.ClickHouse<TestEntity>()
            .Select(x => x.Id)
            .RightJoin<ClickHouseJoinEntity, TestEntity>(
                j => j.ParentId,
                p => p.Id)
            .ToDebugSql();

        Assert.Equal("SELECT \"test_table\".\"id\" AS \"Id\" FROM \"test_table\" RIGHT JOIN \"ch_join_table\" ON \"ch_join_table\".\"parent_id\" = \"test_table\".\"id\";", sql);
    }

    [Fact]
    public void FullJoin_ReturnsCorrectSql()
    {
        var sql = VeloxRuntime.ClickHouse<TestEntity>()
            .Select(x => x.Id)
            .FullJoin<ClickHouseJoinEntity, TestEntity>(
                j => j.ParentId,
                p => p.Id)
            .ToDebugSql();

        Assert.Equal("SELECT \"test_table\".\"id\" AS \"Id\" FROM \"test_table\" FULL JOIN \"ch_join_table\" ON \"ch_join_table\".\"parent_id\" = \"test_table\".\"id\";", sql);
    }

    [Fact]
    public void CrossJoin_ReturnsCorrectSql()
    {
        var sql = VeloxRuntime.ClickHouse<TestEntity>()
            .Select(x => x.Id)
            .CrossJoin<ClickHouseJoinEntity>()
            .ToDebugSql();

        Assert.Equal("SELECT \"test_table\".\"id\" AS \"Id\" FROM \"test_table\" CROSS JOIN \"ch_join_table\";", sql);
    }
}
