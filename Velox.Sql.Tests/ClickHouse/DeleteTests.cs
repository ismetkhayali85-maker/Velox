#pragma warning disable CS0618

namespace Velox.Sql.Tests.ClickHouse;

public class DeleteTests : TestBase
{
    [Fact]
    public void Delete_ReturnsCorrectSql()
    {
        var sql = VeloxRuntime.ClickHouse<TestEntity>()
            .Delete(x => x.Id == 1)
            .ToDebugSql();

        Assert.Equal("ALTER TABLE \"test_table\" DELETE WHERE \"test_table\".\"id\" = 1;", sql);
    }
}
