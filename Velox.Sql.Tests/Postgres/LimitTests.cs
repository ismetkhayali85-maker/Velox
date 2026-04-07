#pragma warning disable CS0618

namespace Velox.Sql.Tests.Postgres;

public class LimitTests : TestBase
{
    [Fact]
    public void LimitOffset_ReturnsCorrectSql()
    {
        var sql = DbQuery<PostgresTestEntity>.GetPostgresBuilder()
            .Limit(10)
            .Offset(5)
            .ToDebugSql();

        Assert.Equal("SELECT \"pg_table\".\"id\" AS \"Id\", \"pg_table\".\"description\" AS \"Description\" FROM \"pg_table\" LIMIT 10 OFFSET 5;", sql);
    }
}
