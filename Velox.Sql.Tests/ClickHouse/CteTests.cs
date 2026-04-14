#pragma warning disable CS0618

namespace Velox.Sql.Tests.ClickHouse;

public class CteTests : TestBase
{
    [Fact]
    public void With_OneCte_ReturnsCorrectSql()
    {
        var sql = VeloxRuntime.ClickHouse<TestEntity>()
            .With<TestEntity>("r", s => s.Select(x => x.Id).Where(w => w.Id == 1))
            .FromCte("r")
            .AddValue("\"r\".\"id\"", "Id", false)
            .ToDebugSql();

        Assert.Equal(
            "WITH \"r\" AS (SELECT \"test_table\".\"id\" AS \"Id\" FROM \"test_table\" WHERE \"test_table\".\"id\" = 1) SELECT \"r\".\"id\" AS \"Id\" FROM \"r\";",
            sql);
    }

    [Fact]
    public void With_TwoCtes_ReturnsCorrectSql()
    {
        var sql = VeloxRuntime.ClickHouse<TestEntity>()
            .With<TestEntity>("a", s => s.Select(x => x.Id).Where(w => w.Id == 1))
            .With<TestEntity>("b", s => s.Select(x => x.Name))
            .FromCte("b")
            .AddValue("\"b\".\"name\"", "Name", false)
            .ToDebugSql();

        Assert.Equal(
            "WITH \"a\" AS (SELECT \"test_table\".\"id\" AS \"Id\" FROM \"test_table\" WHERE \"test_table\".\"id\" = 1), " +
            "\"b\" AS (SELECT \"test_table\".\"name\" AS \"Name\" FROM \"test_table\") " +
            "SELECT \"b\".\"name\" AS \"Name\" FROM \"b\";",
            sql);
    }

    [Fact]
    public void WithRecursive_RawSql_ReturnsCorrectSql()
    {
        var sql = VeloxRuntime.ClickHouse<TestEntity>()
            .WithRecursive("cnt", "SELECT 1 AS n UNION ALL SELECT n + 1 FROM cnt WHERE n < 3")
            .FromCte("cnt")
            .AddValue("\"cnt\".\"n\"", "n", false)
            .ToDebugSql();

        Assert.Equal(
            "WITH RECURSIVE \"cnt\" AS (SELECT 1 AS n UNION ALL SELECT n + 1 FROM cnt WHERE n < 3) " +
            "SELECT \"cnt\".\"n\" AS \"n\" FROM \"cnt\";",
            sql);
    }
}
