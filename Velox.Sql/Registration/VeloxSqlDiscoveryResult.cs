using System.Collections.Generic;
using Velox.Sql.Impl;
using Velox.Sql.Impl.Map;

namespace Velox.Sql.Registration;

/// <summary>
/// Outcome of <see cref="VeloxSqlMapperDiscovery.Discover(System.Reflection.Assembly[])"/>:
/// mapper instances grouped per engine, plus helpers to build configurations or assign <see cref="DbQuery"/> defaults.
/// </summary>
public sealed class VeloxSqlDiscoveryResult
{
    internal VeloxSqlDiscoveryResult(
        IReadOnlyList<IClassMapper> postgresMappers,
        IReadOnlyList<IClassMapper> clickHouseMappers)
    {
        PostgresMappers = postgresMappers;
        ClickHouseMappers = clickHouseMappers;
    }

    /// <summary>Mappers discovered for PostgreSQL.</summary>
    public IReadOnlyList<IClassMapper> PostgresMappers { get; }

    /// <summary>Mappers discovered for ClickHouse.</summary>
    public IReadOnlyList<IClassMapper> ClickHouseMappers { get; }

    public PgSqlConfiguration CreatePostgresConfiguration() =>
        new PgSqlConfiguration(new List<IClassMapper>(PostgresMappers));

    public ClickHouseSqlConfiguration CreateClickHouseConfiguration() =>
        new ClickHouseSqlConfiguration(new List<IClassMapper>(ClickHouseMappers));

    /// <summary>
    /// Assigns <see cref="DbQuery.DefaultPostgresConfig"/> and <see cref="DbQuery.DefaultClickHouseConfig"/>
    /// from this discovery result (convenience for non-DI hosts).
    /// </summary>
    public void ApplyToDbQuery()
    {
        DbQuery.DefaultPostgresConfig = CreatePostgresConfiguration();
        DbQuery.DefaultClickHouseConfig = CreateClickHouseConfiguration();
    }
}
