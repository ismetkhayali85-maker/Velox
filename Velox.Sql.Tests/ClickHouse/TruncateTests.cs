#pragma warning disable CS0618

namespace Velox.Sql.Tests.ClickHouse;

public class TruncateTests : TestBase
{
    [Fact]
    public void Truncate_Default_ReturnsTruncateTableOnly()
    {
        var sql = VeloxRuntime.ClickHouse<TestEntity>()
            .Truncate()
            .ToDebugSql();

        Assert.Equal("TRUNCATE TABLE \"test_table\";", sql);
    }

    [Fact]
    public void Truncate_IfExists_PrefixesClause()
    {
        var sql = VeloxRuntime.ClickHouse<TestEntity>()
            .Truncate()
            .IfExists()
            .ToDebugSql();

        Assert.Equal("TRUNCATE TABLE IF EXISTS \"test_table\";", sql);
    }

    [Fact]
    public void Truncate_OnCluster_AppendsCluster()
    {
        var sql = VeloxRuntime.ClickHouse<TestEntity>()
            .Truncate()
            .OnCluster("replica_cluster")
            .ToDebugSql();

        Assert.Equal("TRUNCATE TABLE \"test_table\" ON CLUSTER \"replica_cluster\";", sql);
    }

    [Fact]
    public void Truncate_IfExistsAndOnCluster_Combines()
    {
        var sql = VeloxRuntime.ClickHouse<TestEntity>()
            .Truncate()
            .IfExists()
            .OnCluster("sharded")
            .ToDebugSql();

        Assert.Equal("TRUNCATE TABLE IF EXISTS \"test_table\" ON CLUSTER \"sharded\";", sql);
    }

    [Fact]
    public void Truncate_OnClusterThenIfExists_SameSql()
    {
        var sql = VeloxRuntime.ClickHouse<TestEntity>()
            .Truncate()
            .OnCluster("sharded")
            .IfExists()
            .ToDebugSql();

        Assert.Equal("TRUNCATE TABLE IF EXISTS \"test_table\" ON CLUSTER \"sharded\";", sql);
    }

    [Fact]
    public void Truncate_AddSql_AppendsFragment()
    {
        var sql = VeloxRuntime.ClickHouse<TestEntity>()
            .Truncate()
            .AddSql(" /* batch */")
            .ToDebugSql();

        Assert.Equal("TRUNCATE TABLE \"test_table\" /* batch */;", sql);
    }

    [Fact]
    public void Truncate_OnCluster_Empty_Throws()
    {
        Assert.Throws<ArgumentException>(() =>
        {
            VeloxRuntime.ClickHouse<TestEntity>()
                .Truncate()
                .OnCluster("  ");
        });
    }
}
