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
    IWhere<TEntity> IsFalse(Expression<Func<TEntity, object>> expr);
    IWhere<TEntity> IsNotNull(Expression<Func<TEntity, object>> expr);
    IWhere<TEntity> IsNull(Expression<Func<TEntity, object>> expr);
    IWhere<TEntity> IsTrue(Expression<Func<TEntity, object>> expr);
    IWhere<TEntity> Like(Expression<Func<TEntity, object>> expr, object value, bool isNot = false);
    IWhere<TEntity> ILike(Expression<Func<TEntity, object>> expr, object value, bool isNot = false);
    IWhere<TEntity> Exists(IWhereSubQuery subQuery);
    IWhere<TEntity> NotExists(IWhereSubQuery subQuery);
    IWhere<TEntity> SetValue(Expression<Func<TEntity, object>> expr, Operators @operator, object value);

    IWhere<TEntity> Between(Expression<Func<TEntity, object>> expr, object firstValue, object secondValue);
    IWhere<TEntity> In(Expression<Func<TEntity, object>> expr, IEnumerable<object> values, bool isNot = false);
    IWhere<TEntity> In(Expression<Func<TEntity, object>> expr, IWhereSubQuery subQuery);

    IWhere<TEntity> Any(Expression<Func<TEntity, object>> expr, Operators @operator, IWhereSubQuery subQuery);
    IWhere<TEntity> All(Expression<Func<TEntity, object>> expr, Operators @operator, IWhereSubQuery subQuery);
    IWhere<TEntity> Some(Expression<Func<TEntity, object>> expr, Operators @operator, IWhereSubQuery subQuery);
}
