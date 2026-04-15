using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using Velox.Sql.Core.Interfaces;
using Velox.Sql.Core.Impl;
using Velox.Sql.Core.PostgreSql;
using Velox.Sql.Expressions;
using Velox.Sql.Impl.Map;
using Velox.Sql.Interfaces;

namespace Velox.Sql.Impl.Builders.Postgres;

public class PostgresInsertBuilder<TEntity> : PostgresBuilderBase<TEntity>, IPostgresInsertBuilder<TEntity>
{
    protected readonly List<KeyValuePair<Type, Action<IInsert>>> _insertPredicates = new();
    protected readonly List<Action<IReturning>> _returningPredicates = new();

    public PostgresInsertBuilder(PgSqlConfiguration config, PostgreSqlBuilder builder) 
        : base(config, builder)
    {
    }

    public IPostgresInsertBuilder<TEntity> BulkInsert(IEnumerable<TEntity> list)
    {
        IClassMapper obj = _config.GetMap(typeof(TEntity));

        var bulkValue = new List<Dictionary<string, IValue>>();
        foreach (var entity in list)
        {
            var dict = new Dictionary<string, IValue>();
            foreach (PropertyMap item in obj.Properties)
            {
                (Type type, object value) = PropertyReadCache.GetPropValue(entity, item.Name);
                if (item.Ignored || (value == null && !item.IsNullable))
                    continue;

                dict.Add(item.ColumnName, ConvertTo(value));
            }

            bulkValue.Add(dict);
        }

        _insertPredicates.Add(new KeyValuePair<Type, Action<IInsert>>(typeof(TEntity), insert => insert.BulkInsertValuePairs(bulkValue)));
        return this;
    }

    public IPostgresInsertBuilder<TEntity> Returning<T>(Expression<Func<T, object>> expr = null)
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

    public IPostgresInsertBuilder<TEntity> Returning(Expression<Func<TEntity, object>> expr = null)
    {
        return Returning<TEntity>(expr);
    }

    public IPostgresInsertBuilder<TEntity> ReturningAll()
    {
        AppendReturningAllColumns(typeof(TEntity), _returningPredicates);
        return this;
    }

    public IPostgresInsertBuilder<TEntity> AddSql(string sql)
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
        foreach (var prev in _insertPredicates)
            _builder.Insert(_config.GetTable(prev.Key), prev.Value);

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
