using System;
using Velox.Sql.Impl;
using Velox.Sql.Impl.Builders;
using Velox.Sql.Interfaces;
using Velox.Sql.Registration;

namespace Velox.Sql;

/// <summary>
/// Default <see cref="IVeloxSql"/> implementation backed by PostgreSQL and ClickHouse mapper configurations
/// (typically from a single <see cref="VeloxSqlMapperDiscovery.Discover"/> / <see cref="VeloxSqlDiscoveryResult"/>).
/// </summary>
public sealed class VeloxSql : IVeloxSql
{
    private readonly PgSqlConfiguration _postgres;
    private readonly ClickHouseSqlConfiguration _clickHouse;

    public VeloxSql(PgSqlConfiguration postgres, ClickHouseSqlConfiguration clickHouse)
    {
        _postgres = postgres ?? throw new ArgumentNullException(nameof(postgres));
        _clickHouse = clickHouse ?? throw new ArgumentNullException(nameof(clickHouse));
    }

    /// <summary>Builds configurations from discovery once (same mappers for both engines as produced by discovery).</summary>
    public VeloxSql(VeloxSqlDiscoveryResult discovery)
        : this(
            discovery?.CreatePostgresConfiguration() ?? throw new ArgumentNullException(nameof(discovery)),
            discovery.CreateClickHouseConfiguration())
    {
    }

    public IPostgresBuilder<TEntity> Postgres<TEntity>() =>
        new PostgresBuilder<TEntity>(_postgres);

    public IClickHouseBuilder<TEntity> ClickHouse<TEntity>() =>
        new ClickHouseBuilder<TEntity>(_clickHouse);
}
