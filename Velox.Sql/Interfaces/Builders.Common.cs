using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using Velox.Sql.Core.Interfaces;

namespace Velox.Sql.Interfaces;

/// <summary>
/// Specialized base for builders that support WHERE clauses.
/// </summary>
public interface ISqlWhereBuilder<TBuilder, TEntity> : ISqlBuilder
    where TBuilder : ISqlWhereBuilder<TBuilder, TEntity>
{
    TBuilder Where<T>(Expression<Func<T, bool>> whereExpr);
    TBuilder Where(Expression<Func<TEntity, bool>> whereExpr);
    TBuilder Where(Action<IWhere<TEntity>> action);
    TBuilder AddSql(string sql);
}

/// <summary>
/// Specialized base for builders that support ORDER BY and LIMIT clauses.
/// </summary>
public interface ISqlSortBuilder<TBuilder, TEntity> : ISqlBuilder
    where TBuilder : ISqlSortBuilder<TBuilder, TEntity>
{
    TBuilder OrderBy<T>(bool isAsc = true, Expression<Func<T, object>> expr = null, bool sortByAlias = false);
    TBuilder OrderBy(bool isAsc = true, Expression<Func<TEntity, object>> expr = null, bool sortByAlias = false);
    TBuilder Limit(uint value);
    TBuilder Offset(uint value);
}

public interface ISqlSelectBuilder<TBuilder, TEntity> : 
    ISqlWhereBuilder<TBuilder, TEntity>, 
    ISqlSortBuilder<TBuilder, TEntity>
    where TBuilder : ISqlSelectBuilder<TBuilder, TEntity>
{
    TBuilder SelectAll<T>();
    TBuilder Select<T>(Expression<Func<T, object>> action = null, string alias = null);
    TBuilder Select(Expression<Func<TEntity, object>> expr = null, string alias = null);

    TBuilder From<T>(string alias = null);

    TBuilder InnerJoin<TNewTable, TOldTable>(Expression<Func<TNewTable, object>> firstSelector,
        Expression<Func<TOldTable, object>> secondSelector,
        Expression<Func<TNewTable, TOldTable, object>> resultSelector = null);
    TBuilder LeftJoin<TNewTable, TOldTable>(Expression<Func<TNewTable, object>> firstSelector,
        Expression<Func<TOldTable, object>> secondSelector,
        Expression<Func<TNewTable, TOldTable, object>> resultSelector = null);
    TBuilder RightJoin<TNewTable, TOldTable>(Expression<Func<TNewTable, object>> firstSelector,
        Expression<Func<TOldTable, object>> secondSelector,
        Expression<Func<TNewTable, TOldTable, object>> resultSelector = null);
    TBuilder FullJoin<TNewTable, TOldTable>(Expression<Func<TNewTable, object>> firstSelector,
        Expression<Func<TOldTable, object>> secondSelector,
        Expression<Func<TNewTable, TOldTable, object>> resultSelector = null);
    TBuilder CrossJoin<TTable>();

    TBuilder Count(Expression<Func<TEntity, object>> action = null, bool isDistinct = false, string alias = "");
    TBuilder Count(string alias);
    TBuilder Count(Expression<Func<TEntity, object>> expr, string alias);
    TBuilder CountDistinct(Expression<Func<TEntity, object>> expression, string alias = null);
    TBuilder Distinct(Expression<Func<TEntity, object>> action);

    TBuilder GroupBy(Expression<Func<TEntity, object>> expression = null);
    TBuilder Sum(Expression<Func<TEntity, object>> action, string alias = null);
    TBuilder Avg(Expression<Func<TEntity, object>> action, string alias = null);
    TBuilder Min(Expression<Func<TEntity, object>> action, string alias = null);
    TBuilder Max(Expression<Func<TEntity, object>> action, string alias = null);

    TBuilder Having(Action<IHaving<TEntity>> action);

    TBuilder Union(IWhereSubQuery query);
    TBuilder UnionAll(IWhereSubQuery query);
    TBuilder Intersect(IWhereSubQuery query);
    TBuilder Except(IWhereSubQuery query);
}

/// <summary>
/// Specialized base for INSERT operations.
/// </summary>
public interface ISqlInsertBuilder<TBuilder, TEntity> : ISqlBuilder
    where TBuilder : ISqlInsertBuilder<TBuilder, TEntity>
{
    TBuilder BulkInsert(IEnumerable<TEntity> list);
    TBuilder AddSql(string sql);
}

/// <summary>
/// Specialized base for UPDATE operations.
/// </summary>
public interface ISqlUpdateBuilder<TBuilder, TEntity> : ISqlWhereBuilder<TBuilder, TEntity>
    where TBuilder : ISqlUpdateBuilder<TBuilder, TEntity>
{
}

/// <summary>
/// Specialized base for DELETE operations.
/// </summary>
public interface ISqlDeleteBuilder<TBuilder, TEntity> : ISqlWhereBuilder<TBuilder, TEntity>
    where TBuilder : ISqlDeleteBuilder<TBuilder, TEntity>
{
}
