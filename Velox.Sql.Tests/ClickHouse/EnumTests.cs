#pragma warning disable CS0618

namespace Velox.Sql.Tests.ClickHouse;

public class EnumTests : TestBase
{
    [Fact]
    public void Insert_Entity_WritesUnderlyingInteger()
    {
        var entity = new EnumPersistenceEntity { Id = 1, Kind = PersistenceTestEnum.Beta };
        var sql = VeloxRuntime.ClickHouse<EnumPersistenceEntity>()
            .Insert(entity)
            .ToDebugSql();

        Assert.Equal("INSERT INTO \"ch_kind_row\" (\"id\",\"kind\") VALUES (1, 2);", sql);
    }

    [Fact]
    public void BulkInsert_WritesUnderlyingInteger()
    {
        var rows = new[]
        {
            new EnumPersistenceEntity { Id = 1, Kind = PersistenceTestEnum.Alpha },
            new EnumPersistenceEntity { Id = 2, Kind = PersistenceTestEnum.Gamma }
        };

        var sql = VeloxRuntime.ClickHouse<EnumPersistenceEntity>()
            .BulkInsert(rows)
            .ToDebugSql();

        Assert.Equal("INSERT INTO \"ch_kind_row\" (\"id\",\"kind\") VALUES (1, 0),(2, 5);", sql);
    }

    [Fact]
    public void Update_Entity_WritesUnderlyingInteger()
    {
        var entity = new EnumPersistenceEntity { Id = 1, Kind = PersistenceTestEnum.Gamma };
        var sql = VeloxRuntime.ClickHouse<EnumPersistenceEntity>()
            .Update(entity, x => x.Id == 1)
            .ToDebugSql();

        Assert.Equal(
            "ALTER TABLE \"ch_kind_row\" UPDATE \"id\" = 1, \"kind\" = 5 WHERE \"ch_kind_row\".\"id\" = 1;",
            sql);
    }

    [Fact]
    public void Insert_NullableEnum_IncludesNull()
    {
        var entity = new EnumNullablePersistenceEntity { Id = 1, Kind = null };
        var sql = VeloxRuntime.ClickHouse<EnumNullablePersistenceEntity>()
            .Insert(entity)
            .ToDebugSql();

        Assert.Equal("INSERT INTO \"ch_kind_null_row\" (\"id\",\"kind\") VALUES (1, NULL);", sql);
    }

    [Fact]
    public void Where_Comparison_UsesUnderlyingInteger()
    {
        var sql = VeloxRuntime.ClickHouse<EnumPersistenceEntity>()
            .Select(x => x.Id)
            .From<EnumPersistenceEntity>()
            .Where(x => x.Kind == PersistenceTestEnum.Beta)
            .ToDebugSql();

        Assert.Equal(
            "SELECT \"ch_kind_row\".\"id\" AS \"Id\" FROM \"ch_kind_row\" WHERE \"ch_kind_row\".\"kind\" = 2;",
            sql);
    }
}
