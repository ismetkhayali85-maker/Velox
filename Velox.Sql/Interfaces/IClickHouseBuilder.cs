using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using Velox.Sql.Core.ClickHouseSql.Select;

namespace Velox.Sql.Interfaces;

public interface IClickHouseBuilder<TEntity> : IClickHouseSelectBuilder<TEntity>
{
    IClickHouseInsertBuilder<TEntity> Insert(TEntity entity);
    IClickHouseInsertBuilder<TEntity> BulkInsert(IEnumerable<TEntity> list);
    IClickHouseUpdateBuilder<TEntity> Update(TEntity entity, Expression<Func<TEntity, bool>> whereExpr);
    IClickHouseDeleteBuilder<TEntity> Delete(Expression<Func<TEntity, bool>> deleteExpr);
    IClickHouseTruncateBuilder<TEntity> Truncate();
}

public interface IClickHouseSelectBuilder<TEntity> : ISqlSelectBuilder<IClickHouseSelectBuilder<TEntity>, TEntity>
{
    IClickHouseSelectBuilder<TEntity> Select(Action<IClickHouseSelect> action);
    IClickHouseSelectBuilder<TEntity> Final();
    IClickHouseSelectBuilder<TEntity> AddValue(string value, string alias = null, bool isNeedQuotes = false);
    IClickHouseSelectBuilder<TEntity> AddWhereValue(string value, bool? isAnd = null);
    IClickHouseSelectBuilder<TEntity> Any(Expression<Func<TEntity, object>> action, string alias = null);
    IClickHouseSelectBuilder<TEntity> AnyLast(Expression<Func<TEntity, object>> action, string alias = null);
}

public interface IClickHouseInsertBuilder<TEntity> : ISqlInsertBuilder<IClickHouseInsertBuilder<TEntity>, TEntity>
{
}

public interface IClickHouseUpdateBuilder<TEntity> : ISqlUpdateBuilder<IClickHouseUpdateBuilder<TEntity>, TEntity>
{
    IClickHouseUpdateBuilder<TEntity> AddWhereValue(string value, bool? isAnd = null);
}

public interface IClickHouseDeleteBuilder<TEntity> : ISqlDeleteBuilder<IClickHouseDeleteBuilder<TEntity>, TEntity>
{
}

public interface IClickHouseTruncateBuilder<TEntity> : ISqlBuilder
{
    IClickHouseTruncateBuilder<TEntity> IfExists();
    IClickHouseTruncateBuilder<TEntity> OnCluster(string clusterName);
    IClickHouseTruncateBuilder<TEntity> AddSql(string sql);
}
