using Velox.Sql.Core.Impl;
using Xunit;

namespace Velox.Sql.Tests.ClickHouse;

public class HavingTests : TestBase
{
    [Fact]
    public void Having_WithCount_ReturnsCorrectSql()
    {
        var sql = VeloxRuntime.ClickHouse<TestEntity>()
            .Select(x => x.Id)
            .GroupBy(x => x.Id)
            .Having(h => h.Count(x => x.Id, Operators.GreaterThan, 1))
            .ToDebugSql();

        Assert.Equal("SELECT \"test_table\".\"id\" AS \"Id\" FROM \"test_table\" GROUP BY \"test_table\".\"id\" HAVING count(\"test_table\".\"id\") > 1;", sql);
    }

    [Fact]
    public void Having_WithSum_ReturnsCorrectSql()
    {
        var sql = VeloxRuntime.ClickHouse<TestEntity>()
            .Select(x => x.Id)
            .GroupBy(x => x.Id)
            .Having(h => h.Sum(x => x.Id, Operators.GreaterThan, 100))
            .ToDebugSql();

        Assert.Equal("SELECT \"test_table\".\"id\" AS \"Id\" FROM \"test_table\" GROUP BY \"test_table\".\"id\" HAVING sum(\"test_table\".\"id\") > 100;", sql);
    }

    [Fact]
    public void Having_WithAvg_ReturnsCorrectSql()
    {
        var sql = VeloxRuntime.ClickHouse<TestEntity>()
            .Select(x => x.Id)
            .GroupBy(x => x.Id)
            .Having(h => h.Avg(x => x.Id, Operators.LessThan, 50))
            .ToDebugSql();

        Assert.Equal("SELECT \"test_table\".\"id\" AS \"Id\" FROM \"test_table\" GROUP BY \"test_table\".\"id\" HAVING avg(\"test_table\".\"id\") < 50;", sql);
    }

    [Fact]
    public void Having_WithMinMax_ReturnsCorrectSql()
    {
        var sql = VeloxRuntime.ClickHouse<TestEntity>()
            .Select(x => x.Id)
            .GroupBy(x => x.Id)
            .Having(h => h.Min(x => x.Id, Operators.Equal, 10).And().Max(x => x.Id, Operators.NotEqual, 0))
            .ToDebugSql();

        Assert.Contains("HAVING min(\"test_table\".\"id\") = 10 AND max(\"test_table\".\"id\") <> 0", sql);
    }

    [Fact]
    public void Having_WithCountDistinct_ReturnsCorrectSql()
    {
        var sql = VeloxRuntime.ClickHouse<TestEntity>()
            .Select(x => x.Id)
            .GroupBy(x => x.Id)
            .Having(h => h.CountDistinct(x => x.Id, Operators.GreaterThan, 5))
            .ToDebugSql();

        // ClickHouse uses uniq() for count(DISTINCT ...) in our implementation
        Assert.Equal("SELECT \"test_table\".\"id\" AS \"Id\" FROM \"test_table\" GROUP BY \"test_table\".\"id\" HAVING uniq(\"test_table\".\"id\") > 5;", sql);
    }

    [Fact]
    public void Having_WithBooleanChecks_ReturnsCorrectSql()
    {
        var sql = VeloxRuntime.ClickHouse<TestEntity>()
            .Select(x => x.Id)
            .GroupBy(x => x.Id)
            .Having(h => h.IsTrue(x => x.Id).And().IsNull(x => x.Name))
            .ToDebugSql();

        Assert.Equal("SELECT \"test_table\".\"id\" AS \"Id\" FROM \"test_table\" GROUP BY \"test_table\".\"id\" HAVING \"test_table\".\"id\" = 'TRUE' AND \"test_table\".\"name\" IS NULL;", sql);
    }
}
