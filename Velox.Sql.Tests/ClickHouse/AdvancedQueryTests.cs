
using Velox.Sql.Core.Impl;

#pragma warning disable CS0618

namespace Velox.Sql.Tests.ClickHouse;

public class AdvancedQueryTests : TestBase
{
    [Fact]
    public void Final_ReturnsCorrectSql()
    {
        var sql = VeloxRuntime.ClickHouse<TestEntity>()
            .Select()
            .Final()
            .ToDebugSql();

        Assert.Equal("SELECT \"test_table\".\"id\" AS \"Id\", \"test_table\".\"name\" AS \"Name\" FROM \"test_table\" FINAL ;", sql);
    }

    [Fact]
    public void AggregatesWithAlias_ReturnsCorrectSql()
    {
        var sql = VeloxRuntime.ClickHouse<TestEntity>()
            .Count(x => x.Id, "TotalCount")
            .Sum(x => x.Id, "TotalSum")
            .ToDebugSql();

        Assert.Contains("COUNT(\"test_table\".\"id\") AS \"TotalCount\"", sql);
        Assert.Contains("sum(\"test_table\".\"id\") AS \"TotalSum\"", sql);
    }

    [Fact]
    public void Having_ReturnsCorrectSql()
    {
        var sql = VeloxRuntime.ClickHouse<TestEntity>()
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
        var query1 = VeloxRuntime.ClickHouse<TestEntity>()
            .Select(x => x.Id);
        var query2 = VeloxRuntime.ClickHouse<TestEntity>()
            .Select(x => x.Id);
        
        var sql = query1.UnionAll(query2)
            .ToDebugSql();

        Assert.Contains("UNION ALL", sql);
    }

    [Fact]
    public void AddValue_ReturnsCorrectSql()
    {
        var sql = VeloxRuntime.ClickHouse<TestEntity>()
            .Select(x => x.Id)
            .AddValue("1", "ConstValue")
            .ToDebugSql();

        Assert.Equal("SELECT \"test_table\".\"id\" AS \"Id\", 1 AS \"ConstValue\" FROM \"test_table\";", sql);
    }

    [Fact]
    public void AddWhereValue_ReturnsCorrectSql()
    {
        var sql = VeloxRuntime.ClickHouse<TestEntity>()
            .Select()
            .Where(x => x.Id == 1)
            .AddWhereValue("is_deleted = 0", isAnd: true)
            .ToDebugSql();

        Assert.Contains("SELECT \"test_table\".\"id\" AS \"Id\", \"test_table\".\"name\" AS \"Name\" FROM \"test_table\" WHERE (\"test_table\".\"id\" = 1) AND is_deleted = 0;", sql);
    }

    [Fact]
    public void AnyRespectNulls_ReturnsCorrectSql()
    {
        var sql = VeloxRuntime.ClickHouse<TestEntity>()
            .AnyRespectNulls(x => x.Name, "city")
            .ToDebugSql();

        Assert.Equal(
            "SELECT any(\"test_table\".\"name\") RESPECT NULLS AS \"city\" FROM \"test_table\";",
            sql);
    }

    [Fact]
    public void Any_WithoutRespectNulls_ReturnsCorrectSql()
    {
        var sql = VeloxRuntime.ClickHouse<TestEntity>()
            .Any(x => x.Name)
            .ToDebugSql();

        Assert.Equal(
            "SELECT any(\"test_table\".\"name\") AS \"Name\" FROM \"test_table\";",
            sql);
    }

    [Fact]
    public void AnyHeavy_ReturnsCorrectSql()
    {
        var sql = VeloxRuntime.ClickHouse<TestEntity>()
            .AnyHeavy(x => x.Id, "res")
            .ToDebugSql();

        Assert.Equal(
            "SELECT anyHeavy(\"test_table\".\"id\") AS \"res\" FROM \"test_table\";",
            sql);
    }

    [Fact]
    public void AnyLastRespectNulls_ReturnsCorrectSql()
    {
        var sql = VeloxRuntime.ClickHouse<TestEntity>()
            .AnyLastRespectNulls(x => x.Name, "last_city")
            .ToDebugSql();

        Assert.Equal(
            "SELECT anyLast(\"test_table\".\"name\") RESPECT NULLS AS \"last_city\" FROM \"test_table\";",
            sql);
    }

    [Fact]
    public void FirstValue_ReturnsCorrectSql()
    {
        var sql = VeloxRuntime.ClickHouse<TestEntity>()
            .FirstValue(x => x.Name)
            .ToDebugSql();

        Assert.Equal(
            "SELECT first_value(\"test_table\".\"name\") AS \"Name\" FROM \"test_table\";",
            sql);
    }

    [Fact]
    public void ArgMax_ReturnsQualifiedSql()
    {
        var sql = VeloxRuntime.ClickHouse<TestEntity>()
            .ArgMax(x => x.Name, x => x.Id, "res")
            .ToDebugSql();

        Assert.Equal(
            "SELECT argMax(\"test_table\".\"name\", \"test_table\".\"id\") AS \"res\" FROM \"test_table\";",
            sql);
    }

    [Fact]
    public void ArgMin_ReturnsQualifiedSql()
    {
        var sql = VeloxRuntime.ClickHouse<TestEntity>()
            .ArgMin(x => x.Name, x => x.Id, "res")
            .ToDebugSql();

        Assert.Equal(
            "SELECT argMin(\"test_table\".\"name\", \"test_table\".\"id\") AS \"res\" FROM \"test_table\";",
            sql);
    }

    [Fact]
    public void ArgAndMax_ReturnsQualifiedSql()
    {
        var sql = VeloxRuntime.ClickHouse<TestEntity>()
            .ArgAndMax(x => x.Name, x => x.Id, "t")
            .ToDebugSql();

        Assert.Equal(
            "SELECT argAndMax(\"test_table\".\"name\", \"test_table\".\"id\") AS \"t\" FROM \"test_table\";",
            sql);
    }

    [Fact]
    public void ArgAndMin_ReturnsQualifiedSql()
    {
        var sql = VeloxRuntime.ClickHouse<TestEntity>()
            .ArgAndMin(x => x.Name, x => x.Id, "t")
            .ToDebugSql();

        Assert.Equal(
            "SELECT argAndMin(\"test_table\".\"name\", \"test_table\".\"id\") AS \"t\" FROM \"test_table\";",
            sql);
    }

    [Fact]
    public void SumCount_ReturnsCorrectSql()
    {
        var sql = VeloxRuntime.ClickHouse<TestEntity>()
            .SumCount(x => x.Id, "sc")
            .ToDebugSql();

        Assert.Equal(
            "SELECT sumCount(\"test_table\".\"id\") AS \"sc\" FROM \"test_table\";",
            sql);
    }

    [Fact]
    public void CountIf_Comparison_ReturnsCorrectSql()
    {
        var sql = VeloxRuntime.ClickHouse<TestEntity>()
            .CountIf(x => x.Id > 60000, "high_salary_count")
            .ToDebugSql();

        Assert.Equal(
            "SELECT countIf(\"test_table\".\"id\" > 60000) AS \"high_salary_count\" FROM \"test_table\";",
            sql);
    }

    [Fact]
    public void CountIf_StringEquality_ReturnsCorrectSql()
    {
        var sql = VeloxRuntime.ClickHouse<TestEntity>()
            .CountIf(x => x.Name == "IT", "it_count")
            .ToDebugSql();

        Assert.Equal(
            "SELECT countIf(\"test_table\".\"name\" = 'IT') AS \"it_count\" FROM \"test_table\";",
            sql);
    }

    [Fact]
    public void CountIf_And_ReturnsCorrectSql()
    {
        var sql = VeloxRuntime.ClickHouse<TestEntity>()
            .CountIf(x => x.Name == "IT" && x.Id > 70000, "cnt")
            .ToDebugSql();

        Assert.Equal(
            "SELECT countIf(\"test_table\".\"name\" = 'IT' AND \"test_table\".\"id\" > 70000) AS \"cnt\" FROM \"test_table\";",
            sql);
    }

    [Fact]
    public void CountIf_RawCondition_ReturnsCorrectSql()
    {
        var sql = VeloxRuntime.ClickHouse<TestEntity>()
            .CountIf("\"test_table\".\"id\" > 60000", "high_salary_count")
            .ToDebugSql();

        Assert.Equal(
            "SELECT countIf(\"test_table\".\"id\" > 60000) AS \"high_salary_count\" FROM \"test_table\";",
            sql);
    }

    [Fact]
    public void SumIf_ReturnsCorrectSql()
    {
        var sql = VeloxRuntime.ClickHouse<TestEntity>()
            .SumIf(x => x.Id, x => x.Id > 60000, "s")
            .ToDebugSql();

        Assert.Equal(
            "SELECT sumIf(\"test_table\".\"id\", \"test_table\".\"id\" > 60000) AS \"s\" FROM \"test_table\";",
            sql);
    }

    [Fact]
    public void SumIf_RawCondition_ReturnsCorrectSql()
    {
        var sql = VeloxRuntime.ClickHouse<TestEntity>()
            .SumIf(x => x.Id, "\"test_table\".\"id\" > 60000", "s")
            .ToDebugSql();

        Assert.Equal(
            "SELECT sumIf(\"test_table\".\"id\", \"test_table\".\"id\" > 60000) AS \"s\" FROM \"test_table\";",
            sql);
    }

    [Fact]
    public void SumIf_ValueExpressionSql_ReturnsCorrectSql()
    {
        var sql = VeloxRuntime.ClickHouse<TestEntity>()
            .SumIf("\"test_table\".\"id\" * \"test_table\".\"id\"", x => x.Name == "IT", "electronics_revenue")
            .ToDebugSql();

        Assert.Equal(
            "SELECT sumIf(\"test_table\".\"id\" * \"test_table\".\"id\", \"test_table\".\"name\" = 'IT') AS \"electronics_revenue\" FROM \"test_table\";",
            sql);
    }

    [Fact]
    public void AvgIf_MinIf_MaxIf_ReturnCorrectSql()
    {
        var sql = VeloxRuntime.ClickHouse<TestEntity>()
            .AvgIf(x => x.Id, x => x.Name == "Furniture", "avg_furniture")
            .MinIf(x => x.Id, x => x.Name == "Furniture", "min_furniture")
            .MaxIf(x => x.Id, x => x.Name == "Electronics", "max_electronics")
            .ToDebugSql();

        Assert.Equal(
            "SELECT avgIf(\"test_table\".\"id\", \"test_table\".\"name\" = 'Furniture') AS \"avg_furniture\", " +
            "minIf(\"test_table\".\"id\", \"test_table\".\"name\" = 'Furniture') AS \"min_furniture\", " +
            "maxIf(\"test_table\".\"id\", \"test_table\".\"name\" = 'Electronics') AS \"max_electronics\" FROM \"test_table\";",
            sql);
    }

    [Fact]
    public void RegionStats_IfAggregates_Chained_ReturnCorrectSql()
    {
        var sql = VeloxRuntime.ClickHouse<TestEntity>()
            .MinIf(x => x.Id, x => x.Name == "North", "min_price_north")
            .MaxIf(x => x.Id, x => x.Name == "North", "max_price_north")
            .AvgIf(x => x.Id, x => x.Name == "North", "avg_price_north")
            .SumIf(x => x.Id, x => x.Name == "North", "total_quantity_north")
            .ToDebugSql();

        const string cond = "\"test_table\".\"name\" = 'North'";
        Assert.Equal(
            "SELECT minIf(\"test_table\".\"id\", " + cond + ") AS \"min_price_north\", " +
            "maxIf(\"test_table\".\"id\", " + cond + ") AS \"max_price_north\", " +
            "avgIf(\"test_table\".\"id\", " + cond + ") AS \"avg_price_north\", " +
            "sumIf(\"test_table\".\"id\", " + cond + ") AS \"total_quantity_north\" FROM \"test_table\";",
            sql);
    }
}
