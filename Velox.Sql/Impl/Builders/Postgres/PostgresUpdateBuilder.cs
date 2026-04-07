using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Runtime.InteropServices;
using Velox.Sql.Core.Interfaces;
using Velox.Sql.Core.Impl;
using Velox.Sql.Core.PostgreSql;
using Velox.Sql.Core.PostgreSql.Where;
using Velox.Sql.Expressions;
using Velox.Sql.Impl;
using Velox.Sql.Impl.Map;
using Velox.Sql.Impl.Clauses;
using Velox.Sql.Interfaces;

namespace Velox.Sql.Impl.Builders.Postgres;

public class PostgresUpdateBuilder<TEntity> : PostgresBuilderBase<TEntity>, IPostgresUpdateBuilder<TEntity>
{
    protected readonly List<KeyValuePair<Type, Dictionary<string, object>>> _updatePredicates = new();
    protected readonly List<Action<IWhere>> _wherePredicates = new();
    protected readonly List<Action<IReturning>> _returningPredicates = new();

    public PostgresUpdateBuilder(PgSqlConfiguration config, PostgreSqlBuilder builder) 
        : base(config, builder)
    {
    }

    public IPostgresUpdateBuilder<TEntity> Where<T>(Expression<Func<T, bool>> whereExpr)
    {
        if (whereExpr == null) return this;

        IClassMapper map = _config.GetMap(typeof(T));
        ExpressionResultEx exprSettings = ExpressionParser.FindExpressionEx(whereExpr);

        _wherePredicates.Add(w =>
        {
            var group = new Velox.Sql.Core.PostgreSql.Where.Predicate();
            ExpressionItemEx item = exprSettings.Settings;
            ParseWhereExpression(group, item, map);
            w.Append($"({group.ToSql()})");
        });

        return this;
    }

    public IPostgresUpdateBuilder<TEntity> Where(Expression<Func<TEntity, bool>> whereExpr)
    {
        return Where<TEntity>(whereExpr);
    }

    public IPostgresUpdateBuilder<TEntity> Where(Action<IWhere<TEntity>> action)
    {
        _wherePredicates.Add(w => 
        {
            action(new WhereClause<TEntity>(w, _config, ConvertTo));
        });
        return this;
    }

    public IPostgresUpdateBuilder<TEntity> Returning<T>(Expression<Func<T, object>> expr = null)
    {
        IClassMapper map = _config.GetMap(typeof(T));
        if (expr == null)
            _returningPredicates.Add(r => r.All());
        else
            _returningPredicates.Add(r =>
            {
                MemberUnaryResult[] exprSettings = ExpressionParser.FindMemberUnaryExpression(expr);
                foreach (MemberUnaryResult exprSetting in exprSettings.AsSpan())
                    r.SetValue(new Column(new Table(map.SchemaName, map.TableName), map.GetUserDefinedName(exprSetting.Value), exprSetting.Value));
            });
        return this;
    }

    public IPostgresUpdateBuilder<TEntity> Returning(Expression<Func<TEntity, object>> expr = null)
    {
        return Returning<TEntity>(expr);
    }

    public IPostgresUpdateBuilder<TEntity> ReturningAll()
    {
        IClassMapper map = _config.GetMap(typeof(TEntity));
        var table = new Table(map.SchemaName, map.TableName);
        
        var sortedProperties = map.Properties
            .OrderByDescending(x => x.KeyType != KeyType.NotAKey)
            .ThenByDescending(x => x.Name.Equals("id", StringComparison.OrdinalIgnoreCase))
            .ThenBy(x => x.Name);

        foreach (PropertyMap item in sortedProperties)
            _returningPredicates.Add(r =>
                r.SetValue(new Column(table, item.ColumnName, item.Name)));
        return this;
    }

    public IPostgresUpdateBuilder<TEntity> AddSql(string sql)
    {
        _builder.AddSql(sql);
        return this;
    }

    public void AddUpdatePredicate(Dictionary<string, object> dict)
    {
        _updatePredicates.Add(new KeyValuePair<Type, Dictionary<string, object>>(typeof(TEntity), dict));
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
        _builder = new PostgreSqlBuilder();
        
        foreach (KeyValuePair<Type, Dictionary<string, object>> prev in CollectionsMarshal.AsSpan(_updatePredicates))
            _builder.Update(_config.GetTable(prev.Key), u =>
            {
                foreach (KeyValuePair<string, object> item in prev.Value)
                    u.SetValue(item.Key, ConvertTo(item.Value));
            });

        if (_wherePredicates.Count != 0)
        {
            var group = new Velox.Sql.Core.PostgreSql.Where.Predicate();
            foreach (var action in _wherePredicates)
                action(group);

            if (!group.IsEmpty())
            {
                var sql = group.ToSql();
                if (_wherePredicates.Count == 1 && sql.StartsWith("(") && sql.EndsWith(")"))
                    sql = sql.Substring(1, sql.Length - 2);
                _builder.Where(sql);
            }
        }

        if (_returningPredicates.Count > 0)
        {
            Action<IReturning> returningResult = null;
            foreach (var prev in _returningPredicates)
                returningResult = returningResult == null ? prev : returningResult + prev;
            _builder.Returning(returningResult);
        }

        return withEnd ? _builder.Build() : _builder.BuildWithoutEnd();
    }
}
