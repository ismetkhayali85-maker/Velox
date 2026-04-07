using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using Velox.Sql.Core.ClickHouseSql;
using Velox.Sql.Core.ClickHouseSql.Where;
using Velox.Sql.Core.Impl;
using Velox.Sql.Core.Interfaces;
using Velox.Sql.Expressions;
using Velox.Sql.Impl;
using Velox.Sql.Impl.Map;
using Velox.Sql.Impl.Clauses;
using Velox.Sql.Interfaces;

namespace Velox.Sql.Impl.Builders.ClickHouse;

public class ClickHouseDeleteBuilder<TEntity> : ClickHouseBuilderBase<TEntity>, IClickHouseDeleteBuilder<TEntity>
{
    protected readonly List<Action<ClickHouseWherePredicate>> _wherePredicates = new();

    public ClickHouseDeleteBuilder(ClickHouseSqlConfiguration config, ClickHouseSqlBuilder builder) 
        : base(config, builder)
    {
    }

    public IClickHouseDeleteBuilder<TEntity> Where<T>(Expression<Func<T, bool>> whereExpr)
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

    public IClickHouseDeleteBuilder<TEntity> Where(Expression<Func<TEntity, bool>> whereExpr)
    {
        return Where<TEntity>(whereExpr);
    }

    public IClickHouseDeleteBuilder<TEntity> Where(Action<IWhere<TEntity>> action)
    {
        _wherePredicates.Add(w => 
        {
            action(new WhereClause<TEntity>(w, _config, ConvertTo));
        });
        return this;
    }

    public IClickHouseDeleteBuilder<TEntity> AddSql(string sql)
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
        IClassMapper obj = _config.GetMap(typeof(TEntity));
        var table = new Table(obj.TableName);

        _builder.Delete(table, w => {
            bool first = true;
            foreach (var action in _wherePredicates)
            {
                var temp = new ClickHouseWherePredicate();
                action(temp);
                if (!temp.IsEmpty())
                {
                    if (!first) w.And();
                    var whereSql = temp.ToSql();
                    if (_wherePredicates.Count == 1 && whereSql.StartsWith("(") && whereSql.EndsWith(")"))
                        whereSql = whereSql.Substring(1, whereSql.Length - 2);
                    w.Append(whereSql);
                    first = false;
                }
            }
        });

        return withEnd ? _builder.Build() : _builder.BuildWithoutEnd();
    }
}
