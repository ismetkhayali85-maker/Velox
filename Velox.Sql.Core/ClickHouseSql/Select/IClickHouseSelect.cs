using System;
using Velox.Sql.Core.Interfaces;

namespace Velox.Sql.Core.ClickHouseSql.Select;

public interface IClickHouseSelect
{
    IClickHouseSelect CountAll(string alias);
    IClickHouseSelect Count(IColumn column);
    IClickHouseSelect Distinct(IColumn column);
    IClickHouseSelect Sum(IColumn column);

    /// <summary>ClickHouse <c>sumCount(column)</c>.</summary>
    IClickHouseSelect SumCount(IColumn column);

    /// <summary>ClickHouse <c>sumIf(value, condition)</c> — value is a qualified column (<see cref="IColumn.ShortName"/>) or any SQL expression fragment.</summary>
    IClickHouseSelect SumIf(IColumn column, string conditionSql, string alias = "");

    /// <summary>ClickHouse <c>sumIf(value, condition)</c> with an arbitrary SQL expression for <paramref name="valueExpressionSql"/>.</summary>
    IClickHouseSelect SumIf(string valueExpressionSql, string conditionSql, string alias = "");

    /// <summary>ClickHouse <c>avgIf(value, condition)</c>.</summary>
    IClickHouseSelect AvgIf(IColumn column, string conditionSql, string alias = "");

    /// <summary>ClickHouse <c>avgIf(value, condition)</c> with an arbitrary SQL expression for the first argument.</summary>
    IClickHouseSelect AvgIf(string valueExpressionSql, string conditionSql, string alias = "");

    /// <summary>ClickHouse <c>minIf(value, condition)</c>.</summary>
    IClickHouseSelect MinIf(IColumn column, string conditionSql, string alias = "");

    /// <summary>ClickHouse <c>minIf(value, condition)</c> with an arbitrary SQL expression for the first argument.</summary>
    IClickHouseSelect MinIf(string valueExpressionSql, string conditionSql, string alias = "");

    /// <summary>ClickHouse <c>maxIf(value, condition)</c>.</summary>
    IClickHouseSelect MaxIf(IColumn column, string conditionSql, string alias = "");

    /// <summary>ClickHouse <c>maxIf(value, condition)</c> with an arbitrary SQL expression for the first argument.</summary>
    IClickHouseSelect MaxIf(string valueExpressionSql, string conditionSql, string alias = "");

    /// <summary>ClickHouse <c>countIf(condition)</c>.</summary>
    IClickHouseSelect CountIf(string conditionSql, string alias = "");

    IClickHouseSelect AnyLast(IColumn column);
    IClickHouseSelect Any(IColumn column);

    /// <summary>
    /// ClickHouse: <c>any(column) RESPECT NULLS</c> (only applies to <c>any</c>).
    /// </summary>
    IClickHouseSelect AnyRespectNulls(IColumn column);

    /// <summary>ClickHouse <c>anyHeavy(column)</c>.</summary>
    IClickHouseSelect AnyHeavy(IColumn column);

    /// <summary>ClickHouse <c>first_value(column)</c>.</summary>
    IClickHouseSelect FirstValue(IColumn column);

    /// <summary>ClickHouse <c>anyLast(column) RESPECT NULLS</c>.</summary>
    IClickHouseSelect AnyLastRespectNulls(IColumn column);

    /// <summary>ClickHouse <c>argMax(value, by)</c> — qualified column SQL in <paramref name="value"/> and <paramref name="by"/>.</summary>
    IClickHouseSelect ArgMax(IColumn value, IColumn by, string alias = "");

    /// <summary>ClickHouse <c>argMin(value, by)</c>.</summary>
    IClickHouseSelect ArgMin(IColumn value, IColumn by, string alias = "");

    /// <summary>ClickHouse <c>argAndMax(value, by)</c>.</summary>
    IClickHouseSelect ArgAndMax(IColumn value, IColumn by, string alias = "");

    /// <summary>ClickHouse <c>argAndMin(value, by)</c>.</summary>
    IClickHouseSelect ArgAndMin(IColumn value, IColumn by, string alias = "");

    IClickHouseSelect Column(IColumn column);
    IClickHouseSelect Value(ClickHouseValue value, string alias);
    IClickHouseSelect DistinctCount(IColumn column);
    IClickHouseSelect ToUnixTimestamp(string dateTime, string alias);
    IClickHouseSelect Min(IColumn column);
    IClickHouseSelect Max(IColumn column);
    IClickHouseSelect Avg(IColumn column);
    IClickHouseSelect Function(IColumn column, string functionName, string alias);

    /// <summary>
    /// <c>func(column) OVER (...)</c> — dialect uses lowercase for built-ins where applicable (e.g. <c>sum</c>).
    /// </summary>
    IClickHouseSelect FunctionOver(IColumn column, string funcName, Action<IWindowOver> configure, string alias = "");

    IClickHouseSelect FunctionOver(IColumn column, string funcName, string overClauseSql, string alias = "");

    IClickHouseSelect RowNumberOver(Action<IWindowOver> configure, string alias = "");

    IClickHouseSelect RowNumberOver(string overClauseSql, string alias = "");

    IClickHouseSelect CountAllOver(Action<IWindowOver> configure, string alias = "");

    IClickHouseSelect CountAllOver(string overClauseSql, string alias = "");
}