#pragma warning disable CS0618

namespace Velox.Sql.Tests.Postgres;

public class CteTests : TestBase
{
    [Fact]
    public void With_OneCte_ReturnsCorrectSql()
    {
        var sql = VeloxRuntime.Postgres<PostgresTestEntity>()
            .With<PostgresTestEntity>("r", s => s.Select(x => x.Id).Where(w => w.Id == 1))
            .FromCte("r")
            .Select(s => s.Expression("\"r\".\"Id\" AS \"Id\""))
            .ToDebugSql();

        Assert.Equal(
            "WITH \"r\" AS (SELECT \"pg_table\".\"id\" AS \"Id\" FROM \"pg_table\" WHERE \"pg_table\".\"id\" = 1) SELECT (\"r\".\"Id\" AS \"Id\")  FROM \"r\";",
            sql);
    }

    [Fact]
    public void With_TwoCtes_ReturnsCorrectSql()
    {
        var sql = VeloxRuntime.Postgres<PostgresTestEntity>()
            .With<PostgresTestEntity>("a", s => s.Select(x => x.Id).Where(w => w.Id == 1))
            .With<PostgresTestEntity>("b", s => s.Select(x => x.Description))
            .FromCte("b")
            .Select(s => s.Expression("\"b\".\"Description\" AS \"Description\""))
            .ToDebugSql();

        Assert.Equal(
            "WITH \"a\" AS (SELECT \"pg_table\".\"id\" AS \"Id\" FROM \"pg_table\" WHERE \"pg_table\".\"id\" = 1), " +
            "\"b\" AS (SELECT \"pg_table\".\"description\" AS \"Description\" FROM \"pg_table\") " +
            "SELECT (\"b\".\"Description\" AS \"Description\")  FROM \"b\";",
            sql);
    }

    [Fact]
    public void WithRecursive_RawSql_ReturnsCorrectSql()
    {
        var sql = VeloxRuntime.Postgres<PostgresTestEntity>()
            .WithRecursive("cnt", "SELECT 1 AS n UNION ALL SELECT n + 1 FROM cnt WHERE n < 3")
            .FromCte("cnt")
            .Select(s => s.Expression("\"cnt\".\"n\" AS \"n\""))
            .ToDebugSql();

        Assert.Equal(
            "WITH RECURSIVE \"cnt\" AS (SELECT 1 AS n UNION ALL SELECT n + 1 FROM cnt WHERE n < 3) " +
            "SELECT (\"cnt\".\"n\" AS \"n\")  FROM \"cnt\";",
            sql);
    }

    [Fact]
    public void With_ParameterInCte_MergesParameters()
    {
        var q = VeloxRuntime.Postgres<PostgresTestEntity>()
            .With<PostgresTestEntity>("r", s => s.Select(x => x.Id).Where(w => w.Id == 7))
            .FromCte("r")
            .Select(s => s.Expression("\"r\".\"Id\" AS \"Id\""));

        var sqlQuery = q.ToSql();

        Assert.Contains("WITH \"r\" AS (SELECT", sqlQuery.Sql);
        Assert.Contains("@p", sqlQuery.Sql);
        Assert.True(sqlQuery.Parameters.Count >= 1);
    }
}
