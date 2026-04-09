using Velox.Sql.Impl.Map;
using Velox.Sql.Registration;

namespace Velox.Sql.Tests.Registration;

/// <summary>Intentional conflict: two mappers for one entity on PostgreSQL — used only with <see cref="VeloxSqlMapperDiscovery.DiscoverTypes"/>.</summary>
public sealed class DiscoveryConflictEntity
{
    public int Id { get; set; }
}

[VeloxSqlMapper(SqlEngine.PostgreSQL)]
public sealed class DiscoveryConflictMapperA : Mapper<DiscoveryConflictEntity>
{
    public DiscoveryConflictMapperA()
    {
        Table("conflict_a");
        Map(x => x.Id).Column("id");
        Build();
    }
}

[VeloxSqlMapper(SqlEngine.PostgreSQL)]
public sealed class DiscoveryConflictMapperB : Mapper<DiscoveryConflictEntity>
{
    public DiscoveryConflictMapperB()
    {
        Table("conflict_b");
        Map(x => x.Id).Column("id");
        Build();
    }
}
