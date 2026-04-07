#pragma warning disable CS0618

namespace Velox.Sql.Tests.Postgres;

public class InsertTests : TestBase
{
    [Fact]
    public void Insert_Entity_ReturnsCorrectSql()
    {
        var entity = new PostgresTestEntity { Id = 1, Description = "Test" };
        var sql = DbQuery<PostgresTestEntity>.GetPostgresBuilder()
            .Insert(entity)
            .ToDebugSql();

        Assert.Equal("INSERT INTO \"pg_table\" (\"id\", \"description\") VALUES(1, 'Test');", sql);
    }

    [Fact]
    public void Insert_WithReturning_ReturnsCorrectSql()
    {
        var entity = new PostgresTestEntity { Id = 1, Description = "Test" };
        var sql = DbQuery<PostgresTestEntity>.GetPostgresBuilder()
            .Insert(entity)
            .Returning(x => x.Id)
            .ToDebugSql();

        Assert.Equal("INSERT INTO \"pg_table\" (\"id\", \"description\") VALUES(1, 'Test') RETURNING \"pg_table\".\"id\" AS \"Id\";", sql);
    }

    [Fact]
    public void Insert_WithReturningAll_ReturnsCorrectSql()
    {
        var entity = new PostgresTestEntity { Id = 1, Description = "Test" };
        var sql = DbQuery<PostgresTestEntity>.GetPostgresBuilder()
            .Insert(entity)
            .ReturningAll()
            .ToDebugSql();

        Assert.Equal("INSERT INTO \"pg_table\" (\"id\", \"description\") VALUES(1, 'Test') RETURNING \"pg_table\".\"id\" AS \"Id\", \"pg_table\".\"description\" AS \"Description\";", sql);
    }

    [Fact]
    public void BulkInsert_ReturnsCorrectSql()
    {
        var entities = new List<PostgresTestEntity>
        {
            new PostgresTestEntity { Id = 1, Description = "A" },
            new PostgresTestEntity { Id = 2, Description = "B" }
        };

        var sql = DbQuery<PostgresTestEntity>.GetPostgresBuilder()
            .BulkInsert(entities)
            .ToDebugSql();

        Assert.Equal("INSERT INTO \"pg_table\" (\"id\", \"description\") VALUES(1, 'A'),(2, 'B');", sql);
    }

    [Fact]
    public void Insert_Nullable_IncludesNull_ReturnsCorrectSql()
    {
        var entity = new PostgresNullableTestEntity { Id = 1, Description = null };
        var sql = DbQuery<PostgresNullableTestEntity>.GetPostgresBuilder()
            .Insert(entity)
            .ToDebugSql();

        Assert.Equal("INSERT INTO \"pg_nullable_table\" (\"id\", \"description\") VALUES(1, null);", sql);
    }
}
