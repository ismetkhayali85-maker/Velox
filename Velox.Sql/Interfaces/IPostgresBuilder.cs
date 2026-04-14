using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using Velox.Sql.Core.Interfaces;
using Velox.Sql.Impl.Builders.Postgres;

namespace Velox.Sql.Interfaces;

public interface IPostgresBuilder<TEntity> : IPostgresSelectBuilder<TEntity>
{
    IPostgresInsertBuilder<TEntity> Insert(TEntity entity);
    IPostgresInsertBuilder<TEntity> BulkInsert(IEnumerable<TEntity> list);
    IPostgresUpdateBuilder<TEntity> Update(TEntity entity, Expression<Func<TEntity, bool>> whereExpr);
    IPostgresDeleteBuilder<TEntity> Delete(Expression<Func<TEntity, bool>> deleteExpr);
    IPostgresTruncateBuilder<TEntity> Truncate();
}

public interface IPostgresSelectBuilder<TEntity> : ISqlSelectBuilder<IPostgresSelectBuilder<TEntity>, TEntity>
{
    IPostgresSelectBuilder<TEntity> Select(Action<ISelect> action);
    IPostgresSelectBuilder<TEntity> SubQuery<TSub>(Action<IPostgresBuilder<TSub>> action, string alias = null);

    IPostgresSelectBuilder<TEntity> With<TSub>(string alias, Action<IPostgresBuilder<TSub>> action);

    /// <summary>
    /// Recursive CTE body (SQL inside the parentheses), e.g. anchor <c>UNION ALL</c> recursive part.
    /// </summary>
    IPostgresSelectBuilder<TEntity> WithRecursive(string alias, string innerSelectSql);

    IPostgresSelectBuilder<TEntity> SumOver<TSum>(Expression<Func<TSum, object>> sumExpr, Action<PostgresWindowOverClause> window, string alias = null);

    IPostgresSelectBuilder<TEntity> AvgOver<TAvg>(Expression<Func<TAvg, object>> expr, Action<PostgresWindowOverClause> window, string alias = null);

    IPostgresSelectBuilder<TEntity> MinOver<TMin>(Expression<Func<TMin, object>> expr, Action<PostgresWindowOverClause> window, string alias = null);

    IPostgresSelectBuilder<TEntity> MaxOver<TMax>(Expression<Func<TMax, object>> expr, Action<PostgresWindowOverClause> window, string alias = null);

    IPostgresSelectBuilder<TEntity> RowNumberOver(Action<PostgresWindowOverClause> window, string alias = "RowNumber");

    IPostgresSelectBuilder<TEntity> CountOver(Action<PostgresWindowOverClause> window, string alias = null);
}

public interface IPostgresInsertBuilder<TEntity> : ISqlInsertBuilder<IPostgresInsertBuilder<TEntity>, TEntity>
{
    IPostgresInsertBuilder<TEntity> Returning<T>(Expression<Func<T, object>> expr = null);
    IPostgresInsertBuilder<TEntity> Returning(Expression<Func<TEntity, object>> expr = null);
    IPostgresInsertBuilder<TEntity> ReturningAll();
}

public interface IPostgresUpdateBuilder<TEntity> : ISqlUpdateBuilder<IPostgresUpdateBuilder<TEntity>, TEntity>
{
    IPostgresUpdateBuilder<TEntity> Returning<T>(Expression<Func<T, object>> expr = null);
    IPostgresUpdateBuilder<TEntity> Returning(Expression<Func<TEntity, object>> expr = null);
    IPostgresUpdateBuilder<TEntity> ReturningAll();
}

public interface IPostgresDeleteBuilder<TEntity> : ISqlDeleteBuilder<IPostgresDeleteBuilder<TEntity>, TEntity>
{
    IPostgresDeleteBuilder<TEntity> Returning<T>(Expression<Func<T, object>> expr = null);
    IPostgresDeleteBuilder<TEntity> Returning(Expression<Func<TEntity, object>> expr = null);
    IPostgresDeleteBuilder<TEntity> ReturningAll();
}

public interface IPostgresTruncateBuilder<TEntity> : ISqlBuilder
{
    IPostgresTruncateBuilder<TEntity> RestartIdentity();
    IPostgresTruncateBuilder<TEntity> ContinueIdentity();
    IPostgresTruncateBuilder<TEntity> Cascade();
    IPostgresTruncateBuilder<TEntity> Restrict();
    IPostgresTruncateBuilder<TEntity> AddSql(string sql);
}
