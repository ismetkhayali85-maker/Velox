using Velox.Sql.Impl.Map;
using Velox.Sql.Registration;

namespace Velox.Sql.Tests.Support;

/// <summary>
/// Reference mappers for discovery/DI tests — keep this assembly free of conflicting duplicate entity mappings.
/// </summary>
public sealed class DiscoveryPgEntity
{
    public int Id { get; set; }
    public string Label { get; set; } = "";
}

[VeloxSqlMapper(SqlEngine.PostgreSQL)]
public sealed class DiscoveryPgEntityMapper : Mapper<DiscoveryPgEntity>
{
    public DiscoveryPgEntityMapper()
    {
        Table("discovery_pg");
        Map(x => x.Id).Column("id");
        Map(x => x.Label).Column("label");
        Build();
    }
}

public sealed class DiscoveryChEntity
{
    public int Id { get; set; }
    public string Code { get; set; } = "";
}

[VeloxSqlMapper(SqlEngine.ClickHouse)]
public sealed class DiscoveryChEntityMapper : Mapper<DiscoveryChEntity>
{
    public DiscoveryChEntityMapper()
    {
        Table("discovery_ch");
        Map(x => x.Id).Column("id");
        Map(x => x.Code).Column("code");
        Build();
    }
}

public sealed class DiscoverySharedEntity
{
    public int Id { get; set; }
}

[VeloxSqlMapper(SqlEngine.PostgreSQL | SqlEngine.ClickHouse)]
public sealed class DiscoverySharedEntityMapper : Mapper<DiscoverySharedEntity>
{
    public DiscoverySharedEntityMapper()
    {
        Table("discovery_shared");
        Map(x => x.Id).Column("id");
        Build();
    }
}

/// <summary>Has <see cref="IClassMapper"/> but no attribute — must be ignored by discovery.</summary>
public sealed class DiscoveryUnmarkedEntity
{
    public int Id { get; set; }
}

public sealed class DiscoveryUnmarkedEntityMapper : Mapper<DiscoveryUnmarkedEntity>
{
    public DiscoveryUnmarkedEntityMapper()
    {
        Table("unmarked");
        Map(x => x.Id).Column("id");
        Build();
    }
}
