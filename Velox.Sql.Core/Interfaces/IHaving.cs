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
    IHaving<TEntity> SetValue(Expression<Func<TEntity, object>> expr, Operators @operator, object value);
    IHaving<TEntity> Count(Expression<Func<TEntity, object>> expr, Operators @operator, object value);
    IHaving<TEntity> Sum(Expression<Func<TEntity, object>> expr, Operators @operator, object value);
    IHaving<TEntity> Avg(Expression<Func<TEntity, object>> expr, Operators @operator, object value);
    IHaving<TEntity> Min(Expression<Func<TEntity, object>> expr, Operators @operator, object value);
    IHaving<TEntity> Max(Expression<Func<TEntity, object>> expr, Operators @operator, object value);
    IHaving<TEntity> CountDistinct(Expression<Func<TEntity, object>> expr, Operators @operator, object value);
    IHaving<TEntity> IsTrue(Expression<Func<TEntity, object>> expr);
    IHaving<TEntity> IsFalse(Expression<Func<TEntity, object>> expr);
    IHaving<TEntity> IsNull(Expression<Func<TEntity, object>> expr);
    IHaving<TEntity> IsNotNull(Expression<Func<TEntity, object>> expr);
}
