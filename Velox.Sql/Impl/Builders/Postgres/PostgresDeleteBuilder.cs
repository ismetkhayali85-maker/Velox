using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using Velox.Sql.Core.Impl;
using Velox.Sql.Core.Interfaces;
using Velox.Sql.Core.PostgreSql;
using Velox.Sql.Expressions;
using Velox.Sql.Impl.Map;
using Velox.Sql.Impl.Clauses;
using Velox.Sql.Interfaces;

namespace Velox.Sql.Impl.Builders.Postgres;

public class PostgresDeleteBuilder<TEntity> : PostgresBuilderBase<TEntity>, IPostgresDeleteBuilder<TEntity>
{
    protected readonly List<Action<IWhere>> _wherePredicates = new();
    protected readonly List<Action<IReturning>> _returningPredicates = new();

    public PostgresDeleteBuilder(PgSqlConfiguration config, PostgreSqlBuilder builder) 
        : base(config, builder)
    {
    }

    public IPostgresDeleteBuilder<TEntity> Where<T>(Expression<Func<T, bool>> whereExpr)
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

    public IPostgresDeleteBuilder<TEntity> Where(Expression<Func<TEntity, bool>> whereExpr)
    {
        return Where<TEntity>(whereExpr);
    }

    public IPostgresDeleteBuilder<TEntity> Where(Action<IWhere<TEntity>> action)
    {
        _wherePredicates.Add(w => 
        {
            action(new WhereClause<TEntity>(w, _config, ConvertTo));
        });
        return this;
    }

    public IPostgresDeleteBuilder<TEntity> Returning<T>(Expression<Func<T, object>> expr = null)
    {
        if (expr == null)
        {
            AppendReturningAllColumns(typeof(T), _returningPredicates);
            return this;
        }

        IClassMapper map = _config.GetMap(typeof(T));
        _returningPredicates.Add(r =>
        {
            MemberUnaryResult[] exprSettings = ExpressionParser.FindMemberUnaryExpression(expr);
            foreach (MemberUnaryResult exprSetting in exprSettings.AsSpan())
                r.SetValue(new Column(new Table(map.SchemaName, map.TableName), map.GetUserDefinedName(exprSetting.Value), exprSetting.Value));
        });
        return this;
    }

    public IPostgresDeleteBuilder<TEntity> Returning(Expression<Func<TEntity, object>> expr = null)
    {
        return Returning<TEntity>(expr);
    }

    public IPostgresDeleteBuilder<TEntity> ReturningAll()
    {
        AppendReturningAllColumns(typeof(TEntity), _returningPredicates);
        return this;
    }

    public IPostgresDeleteBuilder<TEntity> AddSql(string sql)
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
        _builder = new PostgreSqlBuilder();
        IClassMapper map = _config.GetMap(typeof(TEntity));
        
        _builder.Delete();
        _builder.From(new Table(map.SchemaName, map.TableName));

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
