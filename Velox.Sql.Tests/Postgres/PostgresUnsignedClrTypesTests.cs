namespace Velox.Sql.Tests.Postgres;

public class PostgresUnsignedClrTypesTests : TestBase
{
    [Fact]
    public void Select_Where_UIntConstant_ToSql_ThrowsNotSupported()
    {
        var builder = VeloxRuntime.Postgres<PostgresUnsignedProbeEntity>();
        builder.Select().Where(x => x.UIntProp == 1u);

        Assert.Throws<NotSupportedException>(() => builder.ToSql());
    }

    [Fact]
    public void Select_Where_UIntConstant_ToDebugSql_ThrowsNotSupported()
    {
        var builder = VeloxRuntime.Postgres<PostgresUnsignedProbeEntity>();
        builder.Select().Where(x => x.UIntProp == 1u);

        Assert.Throws<NotSupportedException>(() => builder.ToDebugSql());
    }

    [Fact]
    public void Select_Where_ULongConstant_ThrowsNotSupported()
    {
        var builder = VeloxRuntime.Postgres<PostgresUnsignedProbeEntity>();
        builder.Select().Where(x => x.ULongProp == 1ul);

        Assert.Throws<NotSupportedException>(() => builder.ToSql());
    }

    [Fact]
    public void Insert_EntityWithUShortProperty_ThrowsNotSupported()
    {
        var entity = new PostgresUShortOnlyEntity { Value = 1 };
        Assert.Throws<NotSupportedException>(() =>
            VeloxRuntime.Postgres<PostgresUShortOnlyEntity>().Insert(entity));
    }

    [Fact]
    public void Insert_EntityWithUIntProperty_ThrowsNotSupported()
    {
        var entity = new PostgresUIntOnlyEntity { Value = 42u };
        Assert.Throws<NotSupportedException>(() =>
            VeloxRuntime.Postgres<PostgresUIntOnlyEntity>().Insert(entity));
    }

    [Fact]
    public void Insert_EntityWithNUIntProperty_ThrowsNotSupported()
    {
        var entity = new PostgresNUIntOnlyEntity { Value = 1 };
        Assert.Throws<NotSupportedException>(() =>
            VeloxRuntime.Postgres<PostgresNUIntOnlyEntity>().Insert(entity));
    }

    [Fact]
    public void Insert_EntityWithUInt128Property_ThrowsNotSupported()
    {
        var entity = new PostgresUInt128OnlyEntity { Value = (UInt128)42 };
        Assert.Throws<NotSupportedException>(() =>
            VeloxRuntime.Postgres<PostgresUInt128OnlyEntity>().Insert(entity));
    }
}
