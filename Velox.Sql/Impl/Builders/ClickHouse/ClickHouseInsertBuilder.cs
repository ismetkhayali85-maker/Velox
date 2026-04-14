using System;
using System.Collections.Generic;
using Velox.Sql.Core.ClickHouseSql;
using Velox.Sql.Core.ClickHouseSql.Insert;
using Velox.Sql.Core.Impl;
using Velox.Sql.Impl.Map;
using Velox.Sql.Interfaces;

namespace Velox.Sql.Impl.Builders.ClickHouse;

public class ClickHouseInsertBuilder<TEntity> : ClickHouseBuilderBase<TEntity>, IClickHouseInsertBuilder<TEntity>
{
    protected readonly List<KeyValuePair<Type, Action<ClickHouseInsertPredicate>>> _insertPredicates = new();

    public ClickHouseInsertBuilder(ClickHouseSqlConfiguration config, ClickHouseSqlBuilder builder) 
        : base(config, builder)
    {
    }

    public IClickHouseInsertBuilder<TEntity> BulkInsert(IEnumerable<TEntity> list)
    {
        IClassMapper obj = _config.GetMap(typeof(TEntity));

        var bulkValue = new List<Dictionary<string, ClickHouseValue>>();
        foreach (var entity in list)
        {
            var dict = new Dictionary<string, ClickHouseValue>();
            foreach (PropertyMap item in obj.Properties)
            {
                (Type type, object value) = PropertyReadCache.GetPropValue(entity, item.Name);
                if (item.Ignored || (value == null && !item.IsNullable))
                    continue;

                ClickHouseValue convertedValue = ConvertValueToClickHouseValue(type, value, value?.ToString(), false);
                dict.Add(item.ColumnName, convertedValue);
            }

            bulkValue.Add(dict);
        }

        _insertPredicates.Add(new KeyValuePair<Type, Action<ClickHouseInsertPredicate>>(typeof(TEntity), insert => insert.BulkInsertValuePairs(bulkValue)));
        return this;
    }

    public IClickHouseInsertBuilder<TEntity> AddSql(string sql)
    {
        _builder.AddSql(sql);
        return this;
    }

    public override SqlQuery ToSql()
    {
        _currentParameters = new Dictionary<string, object>();
        return new SqlQuery
        {
            Sql = GetSqlInternal(true),
            Parameters = _currentParameters
        };
    }

    public override string ToDebugSql()
    {
        _currentParameters = null;
        return GetSqlInternal(true);
    }

    public override string GetSql()
    {
        return GetSqlInternal(false);
    }

    protected string GetSqlInternal(bool withEnd)
    {
        _builder = new ClickHouseSqlBuilder();
        foreach (var prev in _insertPredicates)
            _builder.Insert(_config.GetTable(prev.Key), prev.Value);

        return withEnd ? _builder.Build() : _builder.BuildWithoutEnd();
    }
}
