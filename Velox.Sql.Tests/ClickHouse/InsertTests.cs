namespace Velox.Sql.Tests.ClickHouse;

public class InsertTests : TestBase
{
    [Fact]
    public void Insert_ReturnsCorrectSql()
    {
        var entity = new TestEntity { Id = 1, Name = "test" };
        var sql = VeloxRuntime.ClickHouse<TestEntity>()
            .Insert(entity)
            .ToDebugSql();

        Assert.Equal("INSERT INTO \"test_table\" (\"id\",\"name\") VALUES (1, 'test');", sql);
    }

    [Fact]
    public void BulkInsert_ReturnsCorrectSql()
    {
        var data = new[] { new TestEntity { Id = 1, Name = "A" }, new TestEntity { Id = 2, Name = "B" } };
        var builder = VeloxRuntime.ClickHouse<TestEntity>();
        var insertBuilder = builder.BulkInsert(data);

        AssertQuery(insertBuilder,
            debug: "INSERT INTO \"test_table\" (\"id\",\"name\") VALUES (1, 'A'),(2, 'B');",
            sql:   "INSERT INTO \"test_table\" (\"id\",\"name\") VALUES (1, 'A'),(2, 'B');",
            expectedParams: new { });
    }

    [Fact]
    public void Insert_Nullable_IncludesNull_ReturnsCorrectSql()
    {
        var entity = new NullableTestEntity { Id = 1, Name = null };
        var sql = VeloxRuntime.ClickHouse<NullableTestEntity>()
            .Insert(entity)
            .ToDebugSql();

        Assert.Equal("INSERT INTO \"nullable_test_table\" (\"id\",\"name\") VALUES (1, NULL);", sql);
    }

    [Fact]
    public void BulkInsert_DateTimeNullable_Guid_DateOnly_UsesInvariantClickHouseLiterals()
    {
        var uid = Guid.Parse("7d681c18-9a42-4c9f-b0e8-000000000001");
        var entity = new ClickHouseScalarInsertEntity
        {
            Id = 1,
            At = new DateTime(2026, 5, 12, 0, 0, 0, DateTimeKind.Unspecified),
            Uid = uid,
            Day = new DateOnly(2026, 5, 12)
        };

        var sql = VeloxRuntime.ClickHouse<ClickHouseScalarInsertEntity>()
            .BulkInsert(new[] { entity })
            .ToDebugSql();

        Assert.Equal(
            "INSERT INTO \"ch_scalar_insert\" (\"id\",\"at\",\"uid\",\"day\") VALUES (1, '2026-05-12 00:00:00', '7d681c18-9a42-4c9f-b0e8-000000000001', '2026-05-12');",
            sql);
    }

    [Fact]
    public void BulkInsert_NullableDateTimeAndDateOnly_NullLiterals()
    {
        var uid = Guid.Parse("7d681c18-9a42-4c9f-b0e8-000000000001");
        var entity = new ClickHouseScalarInsertEntity { Id = 2, At = null, Uid = uid, Day = null };

        var sql = VeloxRuntime.ClickHouse<ClickHouseScalarInsertEntity>()
            .BulkInsert(new[] { entity })
            .ToDebugSql();

        Assert.Equal(
            "INSERT INTO \"ch_scalar_insert\" (\"id\",\"at\",\"uid\",\"day\") VALUES (2, NULL, '7d681c18-9a42-4c9f-b0e8-000000000001', NULL);",
            sql);
    }
}
