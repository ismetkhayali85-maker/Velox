using System;

namespace Velox.Sql.Core.Interfaces;

public interface ISelect : ISqlConvertable<ISelect>
{
    ISelect Column(IColumn column);
    ISelect CountAll();
    ISelect Function(IColumn column, string funcName, string funcAlias = "");
    ISelect Function(string funcName, string[] parameters, string funcAlias = "");
    ISelect Expression<TBuilder>(ISqlBuilder<TBuilder> builder);
    ISelect Expression(string sql);
    ISelect Now();
    ISelect Now(string columnAlias);
    ISelect Avg(IColumn column);
    ISelect Count(IColumn column);
    ISelect CountDistinct(IColumn column);
    ISelect DistinctAll();
    ISelect Distinct(IColumn column);
    ISelect DistinctOn(IColumn column);
    ISelect First(IColumn column);
    ISelect Last(IColumn column);
    ISelect Lcase(IColumn column);
    ISelect Ucase(IColumn column);
    ISelect Len(IColumn column);
    ISelect Max(IColumn column);
    ISelect Min(IColumn column);
    ISelect Sum(IColumn column);
    ISelect Mid(IColumn column, int start, int length);
    ISelect Round(IColumn column, int length);

    /// <summary>
    /// <c>func(column) OVER (...)</c> — supply <paramref name="configure"/> to build PARTITION BY / ORDER BY.
    /// </summary>
    ISelect FunctionOver(IColumn column, string funcName, Action<IWindowOver> configure, string funcAlias = "");

    /// <summary>
    /// <c>func(column) OVER (...)</c> with a pre-built window clause (contents inside the parentheses).
    /// </summary>
    ISelect FunctionOver(IColumn column, string funcName, string overClauseSql, string funcAlias = "");

    /// <summary>
    /// <c>ROW_NUMBER() OVER (...)</c>.
    /// </summary>
    ISelect RowNumberOver(Action<IWindowOver> configure, string funcAlias = "");

    /// <summary>
    /// <c>ROW_NUMBER() OVER (...)</c> with a pre-built window clause.
    /// </summary>
    ISelect RowNumberOver(string overClauseSql, string funcAlias = "");

    /// <summary>
    /// <c>COUNT(*) OVER (...)</c>.
    /// </summary>
    ISelect CountAllOver(Action<IWindowOver> configure, string funcAlias = "");

    /// <summary>
    /// <c>COUNT(*) OVER (...)</c> with a pre-built window clause.
    /// </summary>
    ISelect CountAllOver(string overClauseSql, string funcAlias = "");
}