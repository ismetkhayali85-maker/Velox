using System.Collections.Generic;
using Velox.Sql;
using Velox.Sql.Impl;
using Velox.Sql.Impl.Map;

namespace Velox.Sql.Registration;

/// <summary>
/// Outcome of <see cref="VeloxSqlMapperDiscovery.Discover(System.Reflection.Assembly[])"/>:
/// mapper instances grouped per engine, plus helpers to build configurations or <see cref="IVeloxSql"/>.
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

    /// <summary>Creates <see cref="IVeloxSql"/> from this discovery result (same as <c>new VeloxSql(this)</c>).</summary>
    public IVeloxSql CreateVeloxSql() => new VeloxSql(this);
}
