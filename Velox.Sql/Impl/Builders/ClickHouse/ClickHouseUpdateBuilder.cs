using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Runtime.InteropServices;
using Velox.Sql.Core.ClickHouseSql;
using Velox.Sql.Core.ClickHouseSql.Where;
using Velox.Sql.Core.Impl;
using Velox.Sql.Core.Interfaces;
using Velox.Sql.Expressions;
using Velox.Sql.Impl.Map;
using Velox.Sql.Impl.Clauses;
using Velox.Sql.Interfaces;

namespace Velox.Sql.Impl.Builders.ClickHouse;

public class ClickHouseUpdateBuilder<TEntity> : ClickHouseBuilderBase<TEntity>, IClickHouseUpdateBuilder<TEntity>
{
    protected readonly List<KeyValuePair<Type, Dictionary<string, ClickHouseValue>>> _updatePredicates = new();
    protected readonly List<Action<ClickHouseWherePredicate>> _wherePredicates = new();

    public ClickHouseUpdateBuilder(ClickHouseSqlConfiguration config, ClickHouseSqlBuilder builder) 
        : base(config, builder)
    {
    }

    public IClickHouseUpdateBuilder<TEntity> Where<T>(Expression<Func<T, bool>> whereExpr)
    {
        if (whereExpr == null) return this;

        IClassMapper map = _config.GetMap(typeof(T));
        ExpressionResultEx exprSettings = ExpressionParser.FindExpressionEx(whereExpr);

        _wherePredicates.Add(w =>
        {
            var group = new ClickHouseWherePredicate();
            ExpressionItemEx item = exprSettings.Settings;
            ParseWhereExpression(group, item, map);
            w.Append($"({group.ToSql()})");
        });

        return this;
    }

    public IClickHouseUpdateBuilder<TEntity> Where(Expression<Func<TEntity, bool>> whereExpr)
    {
        return Where<TEntity>(whereExpr);
    }

    public IClickHouseUpdateBuilder<TEntity> Where(Action<IWhere<TEntity>> action)
    {
        _wherePredicates.Add(w => 
        {
            action(new WhereClause<TEntity>(w, _config, ConvertTo));
        });
        return this;
    }

    public IClickHouseUpdateBuilder<TEntity> AddSql(string sql)
    {
        _builder.AddSql(sql);
        return this;
    }

    public IClickHouseUpdateBuilder<TEntity> AddWhereValue(string value, bool? isAnd = null)
    {
        _wherePredicates.Add(x => 
        {
            if (isAnd.HasValue)
            {
                if (isAnd.Value) x.And();
                else x.Or();
            }
            x.Append(value);
        });
        return this;
    }

    public void AddUpdatePredicate(Dictionary<string, ClickHouseValue> dict)
    {
        _updatePredicates.Add(new KeyValuePair<Type, Dictionary<string, ClickHouseValue>>(typeof(TEntity), dict));
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
        
        foreach (KeyValuePair<Type, Dictionary<string, ClickHouseValue>> prev in CollectionsMarshal.AsSpan(_updatePredicates))
            _builder.Update(_config.GetTable(prev.Key), u =>
            {
                foreach (KeyValuePair<string, ClickHouseValue> item in prev.Value)
                    u.SetValue(item.Key, item.Value);
            });

        if (_wherePredicates.Count != 0)
        {
            var group = new ClickHouseWherePredicate();
            foreach (var action in _wherePredicates)
                action(group);

            if (!group.IsEmpty())
            {
                var whereSql = group.ToSql();
                if (_wherePredicates.Count == 1 && whereSql.StartsWith("(") && whereSql.EndsWith(")"))
                    whereSql = whereSql.Substring(1, whereSql.Length - 2);
                _builder.Where(whereSql);
            }
        }

        return withEnd ? _builder.Build() : _builder.BuildWithoutEnd();
    }
}
