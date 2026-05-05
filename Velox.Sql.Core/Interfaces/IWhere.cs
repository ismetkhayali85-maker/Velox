using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using Velox.Sql.Core.Impl;
using Velox.Sql.Core.PostgreSql.Where;

namespace Velox.Sql.Core.Interfaces;

public interface IWhere
{
    string ToSql();
    IWhere And();
    IWhere Or();
    IPredicate IsFalse(ITable table, string columnName);
    IPredicate IsNotNull(ITable table, string columnName);
    IPredicate IsNull(ITable table, string columnName);
    IPredicate IsTrue(ITable table, string columnName);
    IPredicate ILike(ITable table, string columnName, IValue value, bool isNot = false);
    IPredicate Like(ITable table, string columnName, IValue value, bool isNot = false);
    IPredicate Not(ITable table, string columnName, IValue value = null);
    IPredicate Any<TBuilder>(ITable table, string columnName, Operators @operator, ISqlBuilder<TBuilder> subQuery);
    IPredicate All<TBuilder>(ITable table, string columnName, Operators @operator, ISqlBuilder<TBuilder> subQuery);
    IPredicate Some<TBuilder>(ITable table, string columnName, Operators @operator, ISqlBuilder<TBuilder> subQuery);
    IPredicate Exists<TBuilder>(ISqlBuilder<TBuilder> subQuery);
    IPredicate NotExists<TBuilder>(ISqlBuilder<TBuilder> subQuery);
    IPredicate SetValue(ITable table, string columnName, Operators @operator, IValue value);
    IPredicate Between(ITable table, string columnName, IValue firstValue, IValue secondValue);
    IPredicate In(ITable table, string columnName, List<IValue> values, bool isNot = false);
    IPredicate In<TBuilder>(ITable table, string columnName, ISqlBuilder<TBuilder> select);
    IPredicate Grouping(Action<IWhere> predicate);
    Predicate GroupingEx(Action<IPredicate> predicate);
    IPredicate Append(IPredicate expr);
    IPredicate Append(string expr);
    bool IsEmpty();
}

public interface IWhere<TEntity>
{
    IWhere<TEntity> And();
    IWhere<TEntity> Or();
    IWhere<TEntity> IsFalse<T>(Expression<Func<TEntity, T>> expr);
    IWhere<TEntity> IsNotNull<T>(Expression<Func<TEntity, T>> expr);
    IWhere<TEntity> IsNull<T>(Expression<Func<TEntity, T>> expr);
    IWhere<TEntity> IsTrue<T>(Expression<Func<TEntity, T>> expr);
    IWhere<TEntity> Like<T>(Expression<Func<TEntity, T>> expr, object value, bool isNot = false);
    IWhere<TEntity> ILike<T>(Expression<Func<TEntity, T>> expr, object value, bool isNot = false);
    IWhere<TEntity> Exists(IWhereSubQuery subQuery);
    IWhere<TEntity> NotExists(IWhereSubQuery subQuery);
    IWhere<TEntity> SetValue<T>(Expression<Func<TEntity, T>> expr, Operators @operator, object value);

    IWhere<TEntity> Between<T>(Expression<Func<TEntity, T>> expr, object firstValue, object secondValue);
    IWhere<TEntity> In<T>(Expression<Func<TEntity, T>> expr, IEnumerable<T> values, bool isNot = false);
    IWhere<TEntity> In<T>(Expression<Func<TEntity, T>> expr, IWhereSubQuery subQuery);

    IWhere<TEntity> Any<T>(Expression<Func<TEntity, T>> expr, Operators @operator, IWhereSubQuery subQuery);
    IWhere<TEntity> All<T>(Expression<Func<TEntity, T>> expr, Operators @operator, IWhereSubQuery subQuery);
    IWhere<TEntity> Some<T>(Expression<Func<TEntity, T>> expr, Operators @operator, IWhereSubQuery subQuery);
}
