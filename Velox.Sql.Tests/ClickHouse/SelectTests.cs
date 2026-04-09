#pragma warning disable CS0618
using Velox.Sql;
using Velox.Sql.Core.ClickHouseSql;

namespace Velox.Sql.Tests.ClickHouse;

public class SelectTests : TestBase
{
    [Fact]
    public void Select_ReturnsCorrectSql()
    {
        var sql = VeloxRuntime.ClickHouse<TestEntity>()
            .Select()
            .ToDebugSql();

        Assert.Equal("SELECT \"test_table\".\"id\" AS \"Id\", \"test_table\".\"name\" AS \"Name\" FROM \"test_table\";", sql);
    }

    [Fact]
    public void SelectSpecificColumn_ReturnsCorrectSql()
    {
        var sql = VeloxRuntime.ClickHouse<TestEntity>()
            .Select(x => x.Name)
            .ToDebugSql();

        Assert.Equal("SELECT \"test_table\".\"name\" AS \"Name\" FROM \"test_table\";", sql);
    }

    [Fact]
    public void SelectMultipleColumns_ReturnsCorrectSql()
    {
        var sql = VeloxRuntime.ClickHouse<TestEntity>()
            .Select(x => new { x.Id, x.Name })
            .ToDebugSql();

        Assert.Equal("SELECT \"test_table\".\"id\" AS \"Id\", \"test_table\".\"name\" AS \"Name\" FROM \"test_table\";", sql);
    }

    [Fact]
    public void Distinct_ReturnsCorrectSql()
    {
        var sql = VeloxRuntime.ClickHouse<TestEntity>()
            .Distinct(x => x.Name)
            .ToDebugSql();

        Assert.Equal("SELECT DISTINCT(\"test_table\".\"name\") AS \"Name\" FROM \"test_table\";", sql);
    }

    [Fact]
    public void Select_Avg_ReturnsCorrectSql()
    {
        var sql = VeloxRuntime.ClickHouse<TestEntity>()
            .Avg(x => x.Id, "AverageId")
            .ToDebugSql();

        Assert.Equal("SELECT avg(\"test_table\".\"id\") AS \"AverageId\" FROM \"test_table\";", sql);
    }

    [Fact]
    public void Select_Sum_ReturnsCorrectSql()
    {
        var sql = VeloxRuntime.ClickHouse<TestEntity>()
            .Sum(x => x.Id, "TotalId")
            .ToDebugSql();

        Assert.Equal("SELECT sum(\"test_table\".\"id\") AS \"TotalId\" FROM \"test_table\";", sql);
    }

    [Fact]
    public void Select_Min_ReturnsCorrectSql()
    {
        var sql = VeloxRuntime.ClickHouse<TestEntity>()
            .Min(x => x.Id, "MinId")
            .ToDebugSql();

        Assert.Equal("SELECT min(\"test_table\".\"id\") AS \"MinId\" FROM \"test_table\";", sql);
    }

    [Fact]
    public void Select_Max_ReturnsCorrectSql()
    {
        var sql = VeloxRuntime.ClickHouse<TestEntity>()
            .Max(x => x.Id, "MaxId")
            .ToDebugSql();

        Assert.Equal("SELECT max(\"test_table\".\"id\") AS \"MaxId\" FROM \"test_table\";", sql);
    }

    [Fact]
    public void Select_Count_ReturnsCorrectSql()
    {
        var sql = VeloxRuntime.ClickHouse<TestEntity>()
            .Count(x => x.Id, false, "IdCount")
            .ToDebugSql();

        Assert.Equal("SELECT COUNT(\"test_table\".\"id\") AS \"IdCount\" FROM \"test_table\";", sql);
    }

    [Fact]
    public void Select_CountAll_ReturnsCorrectSql()
    {
        var sql = VeloxRuntime.ClickHouse<TestEntity>()
            .Count(null, false, "TotalCount")
            .ToDebugSql();

        Assert.Equal("SELECT COUNT(*) AS \"TotalCount\" FROM \"test_table\";", sql);
    }

    [Fact]
    public void Select_DistinctCount_ReturnsCorrectSql()
    {
        var sql = VeloxRuntime.ClickHouse<TestEntity>()
            .CountDistinct(x => x.Name, "UniqueNames")
            .ToDebugSql();

        Assert.Equal("SELECT COUNT( DISTINCT(\"test_table\".\"name\")) AS \"UniqueNames\" FROM \"test_table\";", sql);
    }

    [Fact]
    public void Select_Any_ReturnsCorrectSql()
    {
        var sql = VeloxRuntime.ClickHouse<TestEntity>()
            .Any(x => x.Name, "AnyName")
            .ToDebugSql();

        Assert.Equal("SELECT any(\"test_table\".\"name\") AS \"AnyName\" FROM \"test_table\";", sql);
    }

    [Fact]
    public void Select_AnyLast_ReturnsCorrectSql()
    {
        var sql = VeloxRuntime.ClickHouse<TestEntity>()
            .AnyLast(x => x.Name, "LastEntry")
            .ToDebugSql();

        Assert.Equal("SELECT anyLast(\"test_table\".\"name\") AS \"LastEntry\" FROM \"test_table\";", sql);
    }

    [Fact]
    public void Select_ToUnixTimestamp_ReturnsCorrectSql()
    {
        var sql = VeloxRuntime.ClickHouse<DateTimeEntity>()
            .Select(s => s.ToUnixTimestamp("2023-01-01 00:00:00", "UnixTime"))
            .ToDebugSql();

        Assert.Equal("SELECT toUnixTimestamp('2023-01-01 00:00:00') AS \"UnixTime\" FROM \"date_table\";", sql);
    }

    [Fact]
    public void Select_MixedAggregates_ReturnsCorrectSql()
    {
        var sql = VeloxRuntime.ClickHouse<TestEntity>()
            .Select(s => s
                .Avg(new Column("id", "AvgId"))
                .Sum(new Column("id", "SumId"))
                .Count(new Column("name", "NameCount")))
            .ToDebugSql();

        Assert.Equal("SELECT avg(\"id\") AS \"AvgId\", sum(\"id\") AS \"SumId\", COUNT(\"name\") AS \"NameCount\" FROM \"test_table\";", sql);
    }

    [Fact]
    public void Selection_Security_IdentifierQuoting_ReturnsCorrectSql()
    {
        // verifies that malicious column/table names are safely quoted.
        var maliciousName = "id\", \"secret_field";
        var col = new Column(maliciousName);
        Assert.Equal("\"id\"\", \"\"secret_field\"", col.Name);

        var maliciousTable = "users\"; DROP TABLE students; --";
        var table = new Table(maliciousTable);
        Assert.Equal("\"users\"\"; DROP TABLE students; --\"", table.ToString());
    }
}
