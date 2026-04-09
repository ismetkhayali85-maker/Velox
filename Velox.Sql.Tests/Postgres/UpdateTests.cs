#pragma warning disable CS0618

namespace Velox.Sql.Tests.Postgres;

public class UpdateTests : TestBase
{
    [Fact]
    public void Update_Entity_ReturnsCorrectSql()
    {
        var entity = new PostgresTestEntity { Id = 1, Description = "Updated" };
        var sql = VeloxRuntime.Postgres<PostgresTestEntity>()
            .Update(entity, x => x.Id == 1)
            .ToDebugSql();

        Assert.Equal("UPDATE \"pg_table\" SET \"id\" = 1, \"description\" = 'Updated' WHERE \"pg_table\".\"id\" = 1;", sql);
    }

    [Fact]
    public void Update_Nullable_IncludesNull_ReturnsCorrectSql()
    {
        var entity = new PostgresNullableTestEntity { Id = 1, Description = null };
        var sql = VeloxRuntime.Postgres<PostgresNullableTestEntity>()
            .Update(entity, x => x.Id == 1)
            .ToDebugSql();

        Assert.Equal("UPDATE \"pg_nullable_table\" SET \"id\" = 1, \"description\" = null WHERE \"pg_nullable_table\".\"id\" = 1;", sql);
    }
}
