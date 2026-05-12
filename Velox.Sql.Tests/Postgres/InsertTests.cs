namespace Velox.Sql.Tests.Postgres;

public class InsertTests : TestBase
{
    [Fact]
    public void Insert_Entity_ReturnsCorrectSql()
    {
        var entity = new PostgresTestEntity { Id = 1, Description = "Test" };
        var sql = VeloxRuntime.Postgres<PostgresTestEntity>()
            .Insert(entity)
            .ToDebugSql();

        Assert.Equal("INSERT INTO \"pg_table\" (\"id\", \"description\") VALUES(1, 'Test');", sql);
    }

    [Fact]
    public void Insert_WithReturning_ReturnsCorrectSql()
    {
        var entity = new PostgresTestEntity { Id = 1, Description = "Test" };
        var sql = VeloxRuntime.Postgres<PostgresTestEntity>()
            .Insert(entity)
            .Returning(x => x.Id)
            .ToDebugSql();

        Assert.Equal("INSERT INTO \"pg_table\" (\"id\", \"description\") VALUES(1, 'Test') RETURNING \"pg_table\".\"id\" AS \"Id\";", sql);
    }

    [Fact]
    public void Insert_WithReturningAll_ReturnsCorrectSql()
    {
        var entity = new PostgresTestEntity { Id = 1, Description = "Test" };
        var sql = VeloxRuntime.Postgres<PostgresTestEntity>()
            .Insert(entity)
            .ReturningAll()
            .ToDebugSql();

        Assert.Equal("INSERT INTO \"pg_table\" (\"id\", \"description\") VALUES(1, 'Test') RETURNING \"pg_table\".\"id\" AS \"Id\", \"pg_table\".\"description\" AS \"Description\";", sql);
    }

    [Fact]
    public void Insert_WithReturning_NoExpression_SameAsReturningAll()
    {
        var entity = new PostgresTestEntity { Id = 1, Description = "Test" };
        var sql = VeloxRuntime.Postgres<PostgresTestEntity>()
            .Insert(entity)
            .Returning<PostgresTestEntity>()
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

        var sql = VeloxRuntime.Postgres<PostgresTestEntity>()
            .BulkInsert(entities)
            .ToDebugSql();

        Assert.Equal("INSERT INTO \"pg_table\" (\"id\", \"description\") VALUES(1, 'A'),(2, 'B');", sql);
    }

    [Fact]
    public void Insert_Nullable_IncludesNull_ReturnsCorrectSql()
    {
        var entity = new PostgresNullableTestEntity { Id = 1, Description = null };
        var sql = VeloxRuntime.Postgres<PostgresNullableTestEntity>()
            .Insert(entity)
            .ToDebugSql();

        Assert.Equal("INSERT INTO \"pg_nullable_table\" (\"id\", \"description\") VALUES(1, null);", sql);
    }

    [Fact]
    public void BulkInsert_DateTimeNullable_Guid_DateOnly_UsesIsoLiterals()
    {
        var uid = Guid.Parse("7d681c18-9a42-4c9f-b0e8-000000000001");
        var entity = new PostgresScalarInsertEntity
        {
            Id = 1,
            At = new DateTime(2026, 5, 12, 0, 0, 0, DateTimeKind.Unspecified),
            Uid = uid,
            Day = new DateOnly(2026, 5, 12)
        };

        var sql = VeloxRuntime.Postgres<PostgresScalarInsertEntity>()
            .BulkInsert(new[] { entity })
            .ToDebugSql();

        Assert.Equal(
            "INSERT INTO \"pg_scalar_insert\" (\"id\", \"at\", \"uid\", \"day\") VALUES(1, '2026-05-12 00:00:00', '7d681c18-9a42-4c9f-b0e8-000000000001', '2026-05-12');",
            sql);
    }

    [Fact]
    public void BulkInsert_NullableDateTimeAndDateOnly_NullLiterals()
    {
        var uid = Guid.Parse("7d681c18-9a42-4c9f-b0e8-000000000001");
        var entity = new PostgresScalarInsertEntity { Id = 2, At = null, Uid = uid, Day = null };

        var sql = VeloxRuntime.Postgres<PostgresScalarInsertEntity>()
            .BulkInsert(new[] { entity })
            .ToDebugSql();

        Assert.Equal(
            "INSERT INTO \"pg_scalar_insert\" (\"id\", \"at\", \"uid\", \"day\") VALUES(2, null, '7d681c18-9a42-4c9f-b0e8-000000000001', null);",
            sql);
    }
}
