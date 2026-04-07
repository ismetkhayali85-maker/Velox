
using Velox.Sql.Core.Impl;

#pragma warning disable CS0618

namespace Velox.Sql.Tests.ClickHouse;

public class AdvancedQueryTests : TestBase
{
    [Fact]
    public void Final_ReturnsCorrectSql()
    {
        var sql = DbQuery<TestEntity>.GetClickHouseBuilder()
            .Select()
            .Final()
            .ToDebugSql();

        Assert.Equal("SELECT \"test_table\".\"id\" AS \"Id\", \"test_table\".\"name\" AS \"Name\" FROM \"test_table\" FINAL ;", sql);
    }

    [Fact]
    public void AggregatesWithAlias_ReturnsCorrectSql()
    {
        var sql = DbQuery<TestEntity>.GetClickHouseBuilder()
            .Count(x => x.Id, "TotalCount")
            .Sum(x => x.Id, "TotalSum")
            .ToDebugSql();

        Assert.Contains("COUNT(\"test_table\".\"id\") AS \"TotalCount\"", sql);
        Assert.Contains("sum(\"test_table\".\"id\") AS \"TotalSum\"", sql);
    }

    [Fact]
    public void Having_ReturnsCorrectSql()
    {
        var sql = DbQuery<TestEntity>.GetClickHouseBuilder()
            .Select(x => x.Id)
            .From<TestEntity>()
            .GroupBy(x => x.Id)
            .Having(h => h.Count(x => x.Id, Operators.GreaterThan, 1))
            .ToDebugSql();

        Assert.Contains("HAVING count(\"test_table\".\"id\") > 1", sql);
    }


    [Fact]
    public void UnionAll_ReturnsCorrectSql()
    {
        var query1 = DbQuery<TestEntity>.GetClickHouseBuilder()
            .Select(x => x.Id);
        var query2 = DbQuery<TestEntity>.GetClickHouseBuilder()
            .Select(x => x.Id);
        
        var sql = query1.UnionAll(query2)
            .ToDebugSql();

        Assert.Contains("UNION ALL", sql);
    }

    [Fact]
    public void AddValue_ReturnsCorrectSql()
    {
        var sql = DbQuery<TestEntity>.GetClickHouseBuilder()
            .Select(x => x.Id)
            .AddValue("1", "ConstValue")
            .ToDebugSql();

        Assert.Equal("SELECT \"test_table\".\"id\" AS \"Id\", 1 AS \"ConstValue\" FROM \"test_table\";", sql);
    }

    [Fact]
    public void AddWhereValue_ReturnsCorrectSql()
    {
        var sql = DbQuery<TestEntity>.GetClickHouseBuilder()
            .Select()
            .Where(x => x.Id == 1)
            .AddWhereValue("is_deleted = 0", isAnd: true)
            .ToDebugSql();

        Assert.Contains("SELECT \"test_table\".\"id\" AS \"Id\", \"test_table\".\"name\" AS \"Name\" FROM \"test_table\" WHERE (\"test_table\".\"id\" = 1) AND is_deleted = 0;", sql);
    }
}
