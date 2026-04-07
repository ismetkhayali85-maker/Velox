#pragma warning disable CS0618

namespace Velox.Sql.Tests.Postgres;

public class SortTests : TestBase
{
    [Fact]
    public void OrderBy_Asc_ReturnsCorrectSql()
    {
        var sql = DbQuery<PostgresTestEntity>.GetPostgresBuilder()
            .OrderBy(true, x => x.Id)
            .ToDebugSql();

        Assert.Equal("SELECT \"pg_table\".\"id\" AS \"Id\", \"pg_table\".\"description\" AS \"Description\" FROM \"pg_table\" ORDER BY \"pg_table\".\"id\" ASC;", sql);
    }

    [Fact]
    public void OrderBy_Desc_ReturnsCorrectSql()
    {
        var sql = DbQuery<PostgresTestEntity>.GetPostgresBuilder()
            .OrderBy(false, x => x.Id)
            .ToDebugSql();

        Assert.Equal("SELECT \"pg_table\".\"id\" AS \"Id\", \"pg_table\".\"description\" AS \"Description\" FROM \"pg_table\" ORDER BY \"pg_table\".\"id\" DESC;", sql);
    }
}
