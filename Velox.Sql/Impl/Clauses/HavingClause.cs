using System;
using System.Linq;
using System.Linq.Expressions;
using Velox.Sql.Core.Impl;
using Velox.Sql.Core.Interfaces;
using Velox.Sql.Expressions;

namespace Velox.Sql.Impl.Clauses;

public class HavingClause<TEntity> : IHaving<TEntity>
{
    private readonly IHaving _builder;
    private readonly ISqlConfiguration _config;
    private readonly Func<object, IValue> _converter;

    public HavingClause(IHaving builder, ISqlConfiguration config, Func<object, IValue> converter)
    {
        _builder = builder;
        _config = config;
        _converter = converter;
    }

    public IHaving<TEntity> And()
    {
        _builder.And();
        return this;
    }

    public IHaving<TEntity> Or()
    {
        _builder.Or();
        return this;
    }

    public IHaving<TEntity> SetValue(Expression<Func<TEntity, object>> expr, Operators @operator, object value)
    {
        Resolve(expr, out var table, out var column);
        _builder.SetValue(table, column, @operator, _converter(value));
        return this;
    }


    public IHaving<TEntity> Count(Expression<Func<TEntity, object>> expr, Operators @operator, object value)
    {
        Resolve(expr, out var table, out var column);
        _builder.Count(table, column, @operator, _converter(value));
        return this;
    }

    public IHaving<TEntity> Sum(Expression<Func<TEntity, object>> expr, Operators @operator, object value)
    {
        Resolve(expr, out var table, out var column);
        _builder.Sum(table, column, @operator, _converter(value));
        return this;
    }

    public IHaving<TEntity> Avg(Expression<Func<TEntity, object>> expr, Operators @operator, object value)
    {
        Resolve(expr, out var table, out var column);
        _builder.Avg(table, column, @operator, _converter(value));
        return this;
    }

    public IHaving<TEntity> Min(Expression<Func<TEntity, object>> expr, Operators @operator, object value)
    {
        Resolve(expr, out var table, out var column);
        _builder.Min(table, column, @operator, _converter(value));
        return this;
    }

    public IHaving<TEntity> Max(Expression<Func<TEntity, object>> expr, Operators @operator, object value)
    {
        Resolve(expr, out var table, out var column);
        _builder.Max(table, column, @operator, _converter(value));
        return this;
    }

    public IHaving<TEntity> CountDistinct(Expression<Func<TEntity, object>> expr, Operators @operator, object value)
    {
        Resolve(expr, out var table, out var column);
        _builder.CountDistinct(table, column, @operator, _converter(value));
        return this;
    }

    public IHaving<TEntity> IsTrue(Expression<Func<TEntity, object>> expr)
    {
        Resolve(expr, out var table, out var column);
        _builder.IsTrue(table, column);
        return this;
    }

    public IHaving<TEntity> IsFalse(Expression<Func<TEntity, object>> expr)
    {
        Resolve(expr, out var table, out var column);
        _builder.IsFalse(table, column);
        return this;
    }

    public IHaving<TEntity> IsNull(Expression<Func<TEntity, object>> expr)
    {
        Resolve(expr, out var table, out var column);
        _builder.IsNull(table, column);
        return this;
    }

    public IHaving<TEntity> IsNotNull(Expression<Func<TEntity, object>> expr)
    {
        Resolve(expr, out var table, out var column);
        _builder.IsNotNull(table, column);
        return this;
    }

    private void Resolve(Expression<Func<TEntity, object>> expr, out ITable table, out string column)
    {
        var member = ExpressionParser.FindMemberUnaryExpression(expr).First();
        var map = _config.GetMap(typeof(TEntity));
        table = _config.GetTable(typeof(TEntity));
        column = map.GetUserDefinedName(member.Value);
    }
}
