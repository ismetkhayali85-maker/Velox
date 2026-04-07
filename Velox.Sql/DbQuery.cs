using System;
using Velox.Sql.Impl;
using Velox.Sql.Impl.Builders;
using Velox.Sql.Interfaces;

namespace Velox.Sql;

public static class DbQuery
{
    public static PgSqlConfiguration DefaultPostgresConfig { get; set; }
    public static ClickHouseSqlConfiguration DefaultClickHouseConfig { get; set; }
}

public static class DbQuery<TEntity>
{
    public static IPostgresBuilder<TEntity> GetPostgresBuilder()
    {
        if (DbQuery.DefaultPostgresConfig == null)
            throw new InvalidOperationException("DefaultPostgresConfig is not set in DbQuery.");
            
        return new PostgresBuilder<TEntity>(DbQuery.DefaultPostgresConfig);
    }
    
    public static IPostgresBuilder<TEntity> GetPostgresBuilder(PgSqlConfiguration config)
    {
        return new PostgresBuilder<TEntity>(config);
    }

    public static IClickHouseBuilder<TEntity> GetClickHouseBuilder()
    {
        if (DbQuery.DefaultClickHouseConfig == null)
            throw new InvalidOperationException("DefaultClickHouseConfig is not set in DbQuery.");
            
        return new ClickHouseBuilder<TEntity>(DbQuery.DefaultClickHouseConfig);
    }
    
    public static IClickHouseBuilder<TEntity> GetClickHouseBuilder(ClickHouseSqlConfiguration config)
    {
        return new ClickHouseBuilder<TEntity>(config);
    }
}
