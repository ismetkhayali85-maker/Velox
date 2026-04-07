using Velox.Sql.Core.Impl;

#pragma warning disable CS0618

namespace Velox.Sql.Tests.Postgres;

public class HavingTests : TestBase
{
    [Fact]
    public void Having_WithCount_ReturnsCorrectSql()
    {
        var sql = DbQuery<PostgresTestEntity>.GetPostgresBuilder()
            .Select(x => x.Id)
            .From<PostgresTestEntity>()
            .GroupBy(x => x.Id)
            .Having(h => h.Count(x => x.Id, Operators.GreaterThan, 1))
            .ToDebugSql();

        Assert.Equal("SELECT \"pg_table\".\"id\" AS \"Id\" FROM \"pg_table\" GROUP BY \"pg_table\".\"id\" HAVING count(\"pg_table\".\"id\") > 1;", sql);
    }

    [Fact]
    public void Having_WithSum_ReturnsCorrectSql()
    {
        var sql = DbQuery<PostgresTestEntity>.GetPostgresBuilder()
            .Select(x => x.Id)
            .From<PostgresTestEntity>()
            .GroupBy(x => x.Id)
            .Having(h => h.Sum(x => x.Id, Operators.GreaterThan, 100))
            .ToDebugSql();

        Assert.Equal("SELECT \"pg_table\".\"id\" AS \"Id\" FROM \"pg_table\" GROUP BY \"pg_table\".\"id\" HAVING sum(\"pg_table\".\"id\") > 100;", sql);
    }

    [Fact]
    public void Having_WithAvg_ReturnsCorrectSql()
    {
        var sql = DbQuery<PostgresTestEntity>.GetPostgresBuilder()
            .Select(x => x.Id)
            .From<PostgresTestEntity>()
            .GroupBy(x => x.Id)
            .Having(h => h.Avg(x => x.Id, Operators.LessThan, 50))
            .ToDebugSql();

        Assert.Equal("SELECT \"pg_table\".\"id\" AS \"Id\" FROM \"pg_table\" GROUP BY \"pg_table\".\"id\" HAVING avg(\"pg_table\".\"id\") < 50;", sql);
    }

    [Fact]
    public void Having_WithMin_ReturnsCorrectSql()
    {
        var sql = DbQuery<PostgresTestEntity>.GetPostgresBuilder()
            .Select(x => x.Id)
            .From<PostgresTestEntity>()
            .GroupBy(x => x.Id)
            .Having(h => h.Min(x => x.Id, Operators.Equal, 10))
            .ToDebugSql();

        Assert.Equal("SELECT \"pg_table\".\"id\" AS \"Id\" FROM \"pg_table\" GROUP BY \"pg_table\".\"id\" HAVING min(\"pg_table\".\"id\") = 10;", sql);
    }

    [Fact]
    public void Having_WithMax_ReturnsCorrectSql()
    {
        var sql = DbQuery<PostgresTestEntity>.GetPostgresBuilder()
            .Select(x => x.Id)
            .From<PostgresTestEntity>()
            .GroupBy(x => x.Id)
            .Having(h => h.Max(x => x.Id, Operators.NotEqual, 0))
            .ToDebugSql();

        Assert.Equal("SELECT \"pg_table\".\"id\" AS \"Id\" FROM \"pg_table\" GROUP BY \"pg_table\".\"id\" HAVING max(\"pg_table\".\"id\") <> 0;", sql);
    }

    [Fact]
    public void Having_WithCountDistinct_ReturnsCorrectSql()
    {
        var sql = DbQuery<PostgresTestEntity>.GetPostgresBuilder()
            .Select(x => x.Id)
            .GroupBy(x => x.Id)
            .Having(h => h.CountDistinct(x => x.Id, Operators.GreaterThan, 5))
            .ToDebugSql();

        Assert.Equal("SELECT \"pg_table\".\"id\" AS \"Id\" FROM \"pg_table\" GROUP BY \"pg_table\".\"id\" HAVING count(DISTINCT \"pg_table\".\"id\") > 5;", sql);
    }

    [Fact]
    public void Having_WithBooleanChecks_ReturnsCorrectSql()
    {
        var sql = DbQuery<PostgresTestEntity>.GetPostgresBuilder()
            .Select(x => x.Id)
            .GroupBy(x => x.Id)
            .Having(h => h.IsTrue(x => x.Id).And().IsNull(x => x.Description))
            .ToDebugSql();

        Assert.Equal("SELECT \"pg_table\".\"id\" AS \"Id\" FROM \"pg_table\" GROUP BY \"pg_table\".\"id\" HAVING \"pg_table\".\"id\" = 'TRUE' AND \"pg_table\".\"description\" IS NULL;", sql);
    }

    [Fact]
    public void Having_WithMultipleConditions_ReturnsCorrectSql()
    {
        var sql = DbQuery<PostgresTestEntity>.GetPostgresBuilder()
            .Select(x => x.Id)
            .From<PostgresTestEntity>()
            .GroupBy(x => x.Id)
            .Having(h => h.Count(x => x.Id, Operators.GreaterThan, 1)
                          .Sum(x => x.Id, Operators.LessThan, 1000))
            .ToDebugSql();

        Assert.Equal("SELECT \"pg_table\".\"id\" AS \"Id\" FROM \"pg_table\" GROUP BY \"pg_table\".\"id\" HAVING count(\"pg_table\".\"id\") > 1 AND sum(\"pg_table\".\"id\") < 1000;", sql);
    }
}

