#pragma warning disable CS0618

namespace Velox.Sql.Tests.Postgres;

public class DeleteTests : TestBase
{
    [Fact]
    public void Delete_Specific_ReturnsCorrectSql()
    {
        var sql = DbQuery<PostgresTestEntity>.GetPostgresBuilder()
            .Delete(x => x.Id == 50)
            .ToDebugSql();

        Assert.Equal("DELETE FROM \"pg_table\" WHERE \"pg_table\".\"id\" = 50;", sql);
    }
}
