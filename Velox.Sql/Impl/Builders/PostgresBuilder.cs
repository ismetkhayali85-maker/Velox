using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Runtime.InteropServices;
using Velox.Sql.Core.PostgreSql;
using Velox.Sql.Impl.Builders.Postgres;
using Velox.Sql.Core.Interfaces;
using Velox.Sql.Impl.Map;
using Velox.Sql.Interfaces;

namespace Velox.Sql.Impl.Builders;

public sealed class PostgresBuilder<TEntity> : 
    PostgresSelectBuilder<TEntity>, 
    IPostgresBuilder<TEntity>
{
    public PostgresBuilder(PgSqlConfiguration config) 
        : base(config, new PostgreSqlBuilder())
    {
    }

    public IPostgresInsertBuilder<TEntity> BulkInsert(IEnumerable<TEntity> list)
    {
        var builder = new PostgresInsertBuilder<TEntity>(_config, new PostgreSqlBuilder());
        builder.BulkInsert(list);
        return builder;
    }

    public IPostgresInsertBuilder<TEntity> Insert(TEntity entity)
    {
        var builder = new PostgresInsertBuilder<TEntity>(_config, new PostgreSqlBuilder());
        
        IClassMapper obj = _config.GetMap(typeof(TEntity));
        var dict = new Dictionary<string, IValue>();
        foreach (PropertyMap item in obj.Properties)
        {
            (Type type, object value) = PropertyReadCache.GetPropValue(entity, item.Name);
            if (item.Ignored || (value == null && !item.IsNullable))
                continue;

            dict.Add(item.ColumnName, ConvertTo(value));
        }

        builder.BulkInsert(new List<TEntity> { entity });
        return builder;
    }

    public IPostgresUpdateBuilder<TEntity> Update(TEntity entity, Expression<Func<TEntity, bool>> whereExpr)
    {
        var builder = new PostgresUpdateBuilder<TEntity>(_config, new PostgreSqlBuilder());
        
        IClassMapper obj = _config.GetMap(typeof(TEntity));
        var dict = new Dictionary<string, object>();
        foreach (ref PropertyMap item in CollectionsMarshal.AsSpan(obj.Properties))
        {
            (Type type, object value) = PropertyReadCache.GetPropValue(entity, item.Name);
            if (item.Ignored || (value == null && !item.IsNullable))
                continue;

            dict.Add(item.ColumnName, value);
        }

        builder.AddUpdatePredicate(dict);
        if (whereExpr != null) builder.Where(whereExpr);
        
        return builder;
    }

    public IPostgresDeleteBuilder<TEntity> Delete(Expression<Func<TEntity, bool>> deleteExpr)
    {
        var builder = new PostgresDeleteBuilder<TEntity>(_config, new PostgreSqlBuilder());
        if (deleteExpr != null) builder.Where(deleteExpr);
        return builder;
    }
}