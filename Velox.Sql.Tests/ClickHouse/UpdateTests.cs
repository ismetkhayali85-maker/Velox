#pragma warning disable CS0618

namespace Velox.Sql.Tests.ClickHouse;

public class UpdateTests : TestBase
{
    [Fact]
    public void Update_ReturnsCorrectSql()
    {
        var entity = new TestEntity { Id = 1, Name = "updated" };
        var sql = DbQuery<TestEntity>.GetClickHouseBuilder()
            .Update(entity, x => x.Id == 1)
            .ToDebugSql();

        Assert.Equal("ALTER TABLE \"test_table\" UPDATE \"id\" = 1, \"name\" = 'updated' WHERE \"test_table\".\"id\" = 1;", sql);
    }

    [Fact]
    public void Update_Nullable_IncludesNull_ReturnsCorrectSql()
    {
        var entity = new NullableTestEntity { Id = 1, Name = null };
        var sql = DbQuery<NullableTestEntity>.GetClickHouseBuilder()
            .Update(entity, x => x.Id == 1)
            .ToDebugSql();

        Assert.Equal("ALTER TABLE \"nullable_test_table\" UPDATE \"id\" = 1, \"name\" = NULL WHERE \"nullable_test_table\".\"id\" = 1;", sql);
    }
}
