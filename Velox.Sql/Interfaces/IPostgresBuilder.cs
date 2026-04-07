using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using Velox.Sql.Core.Interfaces;

namespace Velox.Sql.Interfaces;

public interface IPostgresBuilder<TEntity> : IPostgresSelectBuilder<TEntity>
{
    IPostgresInsertBuilder<TEntity> Insert(TEntity entity);
    IPostgresInsertBuilder<TEntity> BulkInsert(IEnumerable<TEntity> list);
    IPostgresUpdateBuilder<TEntity> Update(TEntity entity, Expression<Func<TEntity, bool>> whereExpr);
    IPostgresDeleteBuilder<TEntity> Delete(Expression<Func<TEntity, bool>> deleteExpr);
}

public interface IPostgresSelectBuilder<TEntity> : ISqlSelectBuilder<IPostgresSelectBuilder<TEntity>, TEntity>
{
    IPostgresSelectBuilder<TEntity> Select(Action<ISelect> action);
    IPostgresSelectBuilder<TEntity> SubQuery<TSub>(Action<IPostgresBuilder<TSub>> action, string alias = null);
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
