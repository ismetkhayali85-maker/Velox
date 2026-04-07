using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Runtime.InteropServices;
using Velox.Sql.Core.ClickHouseSql;
using Velox.Sql.Core.ClickHouseSql.Select;
using Velox.Sql.Core.ClickHouseSql.Insert;
using Velox.Sql.Core.ClickHouseSql.Update;
using Velox.Sql.Core.ClickHouseSql.Where;
using Velox.Sql.Impl.Builders.ClickHouse;
using Velox.Sql.Impl.Map;
using Velox.Sql.Interfaces;

namespace Velox.Sql.Impl.Builders;

public sealed class ClickHouseBuilder<TEntity> : 
    ClickHouseSelectBuilder<TEntity>,
    IClickHouseBuilder<TEntity>
{
    public ClickHouseBuilder(ClickHouseSqlConfiguration config) 
        : base(config, new ClickHouseSqlBuilder())
    {
    }

    public IClickHouseInsertBuilder<TEntity> BulkInsert(IEnumerable<TEntity> list)
    {
        var builder = new ClickHouseInsertBuilder<TEntity>(_config, new ClickHouseSqlBuilder());
        builder.BulkInsert(list);
        return builder;
    }

    public IClickHouseInsertBuilder<TEntity> Insert(TEntity entity)
    {
        var builder = new ClickHouseInsertBuilder<TEntity>(_config, new ClickHouseSqlBuilder());
        
        IClassMapper obj = _config.GetMap(typeof(TEntity));
        var dict = new Dictionary<string, ClickHouseValue>();
        foreach (PropertyMap item in obj.Properties)
        {
            (Type type, object value) = PropertyReadCache.GetPropValue(entity, item.Name);
            if (item.Ignored || (value == null && !item.IsNullable))
                continue;

            ClickHouseValue convertedValue = ConvertValueToClickHouseValue(type, value, value?.ToString(), false);
            dict.Add(item.ColumnName, convertedValue);
        }

        builder.BulkInsert(new List<TEntity> { entity });
        return builder;
    }

    public IClickHouseUpdateBuilder<TEntity> Update(TEntity entity, Expression<Func<TEntity, bool>> whereExpr)
    {
        var builder = new ClickHouseUpdateBuilder<TEntity>(_config, new ClickHouseSqlBuilder());
        
        IClassMapper obj = _config.GetMap(typeof(TEntity));
        var dict = new Dictionary<string, ClickHouseValue>();
        foreach (ref PropertyMap item in CollectionsMarshal.AsSpan(obj.Properties))
        {
            (Type type, object value) = PropertyReadCache.GetPropValue(entity, item.Name);
            if (item.Ignored || (value == null && !item.IsNullable))
                continue;

            ClickHouseValue convertedValue = ConvertValueToClickHouseValue(type, value, value?.ToString(), false);
            dict.Add(item.ColumnName, convertedValue);
        }

        builder.AddUpdatePredicate(dict);
        if (whereExpr != null) builder.Where(whereExpr);
        
        return builder;
    }

    public IClickHouseDeleteBuilder<TEntity> Delete(Expression<Func<TEntity, bool>> deleteExpr)
    {
        var builder = new ClickHouseDeleteBuilder<TEntity>(_config, new ClickHouseSqlBuilder());
        if (deleteExpr != null) builder.Where(deleteExpr);
        return builder;
    }
}