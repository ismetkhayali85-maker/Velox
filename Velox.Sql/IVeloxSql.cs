using Velox.Sql.Interfaces;

namespace Velox.Sql;

/// <summary>
/// Entry point for building SQL using mapper configurations (PostgreSQL and ClickHouse).
/// Resolve from DI after <c>AddVeloxSql</c>, or construct <see cref="VeloxSql"/> with explicit configurations.
/// </summary>
public interface IVeloxSql
{
    IPostgresBuilder<TEntity> Postgres<TEntity>();

    IClickHouseBuilder<TEntity> ClickHouse<TEntity>();
}
