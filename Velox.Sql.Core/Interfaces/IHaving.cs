using System;
using System.Linq.Expressions;
using Velox.Sql.Core.Impl;

namespace Velox.Sql.Core.Interfaces;

public interface IHaving
{
    IHaving And();
    IHaving Or();
    IHaving SetValue(ITable table, string column, Operators @operator, IValue value);
    IHaving Count(ITable table, string column, Operators @operator, IValue value);
    IHaving Sum(ITable table, string column, Operators @operator, IValue value);
    IHaving Avg(ITable table, string column, Operators @operator, IValue value);
    IHaving Min(ITable table, string column, Operators @operator, IValue value);
    IHaving Max(ITable table, string column, Operators @operator, IValue value);
    IHaving CountDistinct(ITable table, string column, Operators @operator, IValue value);
    IHaving IsTrue(ITable table, string column);
    IHaving IsFalse(ITable table, string column);
    IHaving IsNull(ITable table, string column);
    IHaving IsNotNull(ITable table, string column);
}

public interface IHaving<TEntity>
{
    IHaving<TEntity> And();
    IHaving<TEntity> Or();
    IHaving<TEntity> SetValue<T>(Expression<Func<TEntity, T>> expr, Operators @operator, object value);
    IHaving<TEntity> Count<T>(Expression<Func<TEntity, T>> expr, Operators @operator, object value);
    IHaving<TEntity> Sum<T>(Expression<Func<TEntity, T>> expr, Operators @operator, object value);
    IHaving<TEntity> Avg<T>(Expression<Func<TEntity, T>> expr, Operators @operator, object value);
    IHaving<TEntity> Min<T>(Expression<Func<TEntity, T>> expr, Operators @operator, object value);
    IHaving<TEntity> Max<T>(Expression<Func<TEntity, T>> expr, Operators @operator, object value);
    IHaving<TEntity> CountDistinct<T>(Expression<Func<TEntity, T>> expr, Operators @operator, object value);
    IHaving<TEntity> IsTrue<T>(Expression<Func<TEntity, T>> expr);
    IHaving<TEntity> IsFalse<T>(Expression<Func<TEntity, T>> expr);
    IHaving<TEntity> IsNull<T>(Expression<Func<TEntity, T>> expr);
    IHaving<TEntity> IsNotNull<T>(Expression<Func<TEntity, T>> expr);
}
