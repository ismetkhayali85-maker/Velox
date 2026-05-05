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

    public IHaving<TEntity> SetValue<T>(Expression<Func<TEntity, T>> expr, Operators @operator, object value)
    {
        Resolve(expr, out var table, out var column);
        _builder.SetValue(table, column, @operator, _converter(value));
        return this;
    }


    public IHaving<TEntity> Count<T>(Expression<Func<TEntity, T>> expr, Operators @operator, object value)
    {
        Resolve(expr, out var table, out var column);
        _builder.Count(table, column, @operator, _converter(value));
        return this;
    }

    public IHaving<TEntity> Sum<T>(Expression<Func<TEntity, T>> expr, Operators @operator, object value)
    {
        Resolve(expr, out var table, out var column);
        _builder.Sum(table, column, @operator, _converter(value));
        return this;
    }

    public IHaving<TEntity> Avg<T>(Expression<Func<TEntity, T>> expr, Operators @operator, object value)
    {
        Resolve(expr, out var table, out var column);
        _builder.Avg(table, column, @operator, _converter(value));
        return this;
    }

    public IHaving<TEntity> Min<T>(Expression<Func<TEntity, T>> expr, Operators @operator, object value)
    {
        Resolve(expr, out var table, out var column);
        _builder.Min(table, column, @operator, _converter(value));
        return this;
    }

    public IHaving<TEntity> Max<T>(Expression<Func<TEntity, T>> expr, Operators @operator, object value)
    {
        Resolve(expr, out var table, out var column);
        _builder.Max(table, column, @operator, _converter(value));
        return this;
    }

    public IHaving<TEntity> CountDistinct<T>(Expression<Func<TEntity, T>> expr, Operators @operator, object value)
    {
        Resolve(expr, out var table, out var column);
        _builder.CountDistinct(table, column, @operator, _converter(value));
        return this;
    }

    public IHaving<TEntity> IsTrue<T>(Expression<Func<TEntity, T>> expr)
    {
        Resolve(expr, out var table, out var column);
        _builder.IsTrue(table, column);
        return this;
    }

    public IHaving<TEntity> IsFalse<T>(Expression<Func<TEntity, T>> expr)
    {
        Resolve(expr, out var table, out var column);
        _builder.IsFalse(table, column);
        return this;
    }

    public IHaving<TEntity> IsNull<T>(Expression<Func<TEntity, T>> expr)
    {
        Resolve(expr, out var table, out var column);
        _builder.IsNull(table, column);
        return this;
    }

    public IHaving<TEntity> IsNotNull<T>(Expression<Func<TEntity, T>> expr)
    {
        Resolve(expr, out var table, out var column);
        _builder.IsNotNull(table, column);
        return this;
    }

    private void Resolve(LambdaExpression expr, out ITable table, out string column)
    {
        var member = ExpressionParser.FindMemberUnaryExpression(expr).First();
        var map = _config.GetMap(typeof(TEntity));
        table = _config.GetTable(typeof(TEntity));
        column = map.GetUserDefinedName(member.Value);
    }
}
