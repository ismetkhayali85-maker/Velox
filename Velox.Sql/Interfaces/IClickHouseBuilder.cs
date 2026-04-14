using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using Velox.Sql.Core.ClickHouseSql.Select;
using Velox.Sql.Impl.Builders.ClickHouse;

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

    IClickHouseSelectBuilder<TEntity> With<TSub>(string alias, Action<IClickHouseBuilder<TSub>> action);

    /// <summary>
    /// Recursive CTE body (SQL inside the parentheses), e.g. anchor <c>UNION ALL</c> recursive part.
    /// </summary>
    IClickHouseSelectBuilder<TEntity> WithRecursive(string alias, string innerSelectSql);

    IClickHouseSelectBuilder<TEntity> Final();
    IClickHouseSelectBuilder<TEntity> AddValue(string value, string alias = null, bool isNeedQuotes = false);
    IClickHouseSelectBuilder<TEntity> AddWhereValue(string value, bool? isAnd = null);
    IClickHouseSelectBuilder<TEntity> Any(Expression<Func<TEntity, object>> action, string alias = null);

    /// <summary>
    /// ClickHouse <c>any(expr) RESPECT NULLS</c> — first row’s value including NULL; see ClickHouse docs for <c>any</c>.
    /// </summary>
    IClickHouseSelectBuilder<TEntity> AnyRespectNulls(Expression<Func<TEntity, object>> action, string alias = null);

    IClickHouseSelectBuilder<TEntity> AnyLast(Expression<Func<TEntity, object>> action, string alias = null);

    /// <summary>ClickHouse <c>anyLast(expr) RESPECT NULLS</c>.</summary>
    IClickHouseSelectBuilder<TEntity> AnyLastRespectNulls(Expression<Func<TEntity, object>> action, string alias = null);

    /// <summary>ClickHouse <c>anyHeavy(expr)</c>.</summary>
    IClickHouseSelectBuilder<TEntity> AnyHeavy(Expression<Func<TEntity, object>> action, string alias = null);

    /// <summary>ClickHouse <c>first_value(expr)</c>.</summary>
    IClickHouseSelectBuilder<TEntity> FirstValue(Expression<Func<TEntity, object>> action, string alias = null);

    /// <summary>ClickHouse <c>argMax(value, by)</c> with qualified columns.</summary>
    IClickHouseSelectBuilder<TEntity> ArgMax(
        Expression<Func<TEntity, object>> valueExpr,
        Expression<Func<TEntity, object>> byExpr,
        string alias = null);

    /// <summary>ClickHouse <c>argMin(value, by)</c>.</summary>
    IClickHouseSelectBuilder<TEntity> ArgMin(
        Expression<Func<TEntity, object>> valueExpr,
        Expression<Func<TEntity, object>> byExpr,
        string alias = null);

    /// <summary>ClickHouse <c>argAndMax(value, by)</c>.</summary>
    IClickHouseSelectBuilder<TEntity> ArgAndMax(
        Expression<Func<TEntity, object>> valueExpr,
        Expression<Func<TEntity, object>> byExpr,
        string alias = null);

    /// <summary>ClickHouse <c>argAndMin(value, by)</c>.</summary>
    IClickHouseSelectBuilder<TEntity> ArgAndMin(
        Expression<Func<TEntity, object>> valueExpr,
        Expression<Func<TEntity, object>> byExpr,
        string alias = null);

    /// <summary>ClickHouse <c>sumCount(expr)</c>.</summary>
    IClickHouseSelectBuilder<TEntity> SumCount(Expression<Func<TEntity, object>> expr, string alias = null);

    /// <summary>ClickHouse <c>countIf(condition)</c> from a boolean expression (same rules as <c>Where</c>).</summary>
    IClickHouseSelectBuilder<TEntity> CountIf<T>(Expression<Func<T, bool>> condition, string alias = null);

    IClickHouseSelectBuilder<TEntity> CountIf(Expression<Func<TEntity, bool>> condition, string alias = null);

    /// <summary>ClickHouse <c>countIf(condition)</c> with a raw condition fragment (e.g. <c>salary &gt; 60000</c>).</summary>
    IClickHouseSelectBuilder<TEntity> CountIf(string conditionSql, string alias = null);

    /// <summary>ClickHouse <c>sumIf(column, condition)</c> from column + boolean expression.</summary>
    IClickHouseSelectBuilder<TEntity> SumIf(
        Expression<Func<TEntity, object>> columnExpr,
        Expression<Func<TEntity, bool>> condition,
        string alias = null);

    /// <summary>ClickHouse <c>sumIf(column, condition)</c> with a raw condition SQL fragment.</summary>
    IClickHouseSelectBuilder<TEntity> SumIf(
        Expression<Func<TEntity, object>> columnExpr,
        string conditionSql,
        string alias = null);

    /// <summary>ClickHouse <c>sumIf(valueExpr, condition)</c> — <paramref name="valueExpressionSql"/> is any SQL for the first argument (e.g. <c>price * quantity</c>).</summary>
    IClickHouseSelectBuilder<TEntity> SumIf(
        string valueExpressionSql,
        Expression<Func<TEntity, bool>> condition,
        string alias = null);

    /// <summary>ClickHouse <c>sumIf(valueExpr, condition)</c> with raw SQL for both parts.</summary>
    IClickHouseSelectBuilder<TEntity> SumIf(string valueExpressionSql, string conditionSql, string alias = null);

    /// <summary>ClickHouse <c>avgIf(value, condition)</c>.</summary>
    IClickHouseSelectBuilder<TEntity> AvgIf(
        Expression<Func<TEntity, object>> columnExpr,
        Expression<Func<TEntity, bool>> condition,
        string alias = null);

    IClickHouseSelectBuilder<TEntity> AvgIf(
        Expression<Func<TEntity, object>> columnExpr,
        string conditionSql,
        string alias = null);

    IClickHouseSelectBuilder<TEntity> AvgIf(
        string valueExpressionSql,
        Expression<Func<TEntity, bool>> condition,
        string alias = null);

    IClickHouseSelectBuilder<TEntity> AvgIf(string valueExpressionSql, string conditionSql, string alias = null);

    /// <summary>ClickHouse <c>minIf(value, condition)</c>.</summary>
    IClickHouseSelectBuilder<TEntity> MinIf(
        Expression<Func<TEntity, object>> columnExpr,
        Expression<Func<TEntity, bool>> condition,
        string alias = null);

    IClickHouseSelectBuilder<TEntity> MinIf(
        Expression<Func<TEntity, object>> columnExpr,
        string conditionSql,
        string alias = null);

    IClickHouseSelectBuilder<TEntity> MinIf(
        string valueExpressionSql,
        Expression<Func<TEntity, bool>> condition,
        string alias = null);

    IClickHouseSelectBuilder<TEntity> MinIf(string valueExpressionSql, string conditionSql, string alias = null);

    /// <summary>ClickHouse <c>maxIf(value, condition)</c>.</summary>
    IClickHouseSelectBuilder<TEntity> MaxIf(
        Expression<Func<TEntity, object>> columnExpr,
        Expression<Func<TEntity, bool>> condition,
        string alias = null);

    IClickHouseSelectBuilder<TEntity> MaxIf(
        Expression<Func<TEntity, object>> columnExpr,
        string conditionSql,
        string alias = null);

    IClickHouseSelectBuilder<TEntity> MaxIf(
        string valueExpressionSql,
        Expression<Func<TEntity, bool>> condition,
        string alias = null);

    IClickHouseSelectBuilder<TEntity> MaxIf(string valueExpressionSql, string conditionSql, string alias = null);

    IClickHouseSelectBuilder<TEntity> SumOver<TSum>(Expression<Func<TSum, object>> sumExpr, Action<ClickHouseWindowOverClause> window, string alias = null);

    IClickHouseSelectBuilder<TEntity> AvgOver<TAvg>(Expression<Func<TAvg, object>> expr, Action<ClickHouseWindowOverClause> window, string alias = null);

    IClickHouseSelectBuilder<TEntity> MinOver<TMin>(Expression<Func<TMin, object>> expr, Action<ClickHouseWindowOverClause> window, string alias = null);

    IClickHouseSelectBuilder<TEntity> MaxOver<TMax>(Expression<Func<TMax, object>> expr, Action<ClickHouseWindowOverClause> window, string alias = null);

    IClickHouseSelectBuilder<TEntity> RowNumberOver(Action<ClickHouseWindowOverClause> window, string alias = "RowNumber");

    IClickHouseSelectBuilder<TEntity> CountOver(Action<ClickHouseWindowOverClause> window, string alias = null);
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
