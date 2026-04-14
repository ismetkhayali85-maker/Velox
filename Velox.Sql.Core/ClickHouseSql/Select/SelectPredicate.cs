using System;
using System.Text;
using Velox.Sql.Core.Impl;
using Velox.Sql.Core.Interfaces;
using Velox.Sql.Core.Windowing;

namespace Velox.Sql.Core.ClickHouseSql.Select;

public sealed class SelectPredicate : IClickHouseSelect
{
    private readonly StringBuilder _sql;
    private bool _isFirst;

    public SelectPredicate()
    {
        _sql = new StringBuilder();
        _isFirst = true;
    }

    public IClickHouseSelect Column(IColumn column)
    {
        IsFirst();

        _sql.Append(column.ShortName);
        if (!string.IsNullOrEmpty(column.Alias))
            _sql.Append(" AS ").AppendIdentifier(column.Alias);
        return this;
    }

    public IClickHouseSelect CountAll(string alias)
    {
        IsFirst();

        _sql.Append("COUNT(*)");
        if (!string.IsNullOrEmpty(alias))
            _sql.Append(" AS ").AppendIdentifier(alias);

        return this;
    }

    public IClickHouseSelect Distinct(IColumn column)
    {
        IsFirst();

        _sql.Append("DISTINCT(").Append(column.ShortName).Append(')');
        if (!string.IsNullOrEmpty(column.Alias))
            _sql.Append(" AS ").AppendIdentifier(column.Alias);
        return this;
    }

    public IClickHouseSelect Count(IColumn column)
    {
        IsFirst();

        _sql.Append("COUNT(").Append(column.ShortName).Append(')');
        if (!string.IsNullOrEmpty(column.Alias))
            _sql.Append(" AS ").AppendIdentifier(column.Alias);
        return this;
    }

    public IClickHouseSelect DistinctCount(IColumn column)
    {
        IsFirst();

        _sql.Append("COUNT( DISTINCT(").Append(column.ShortName).Append("))");
        if (!string.IsNullOrEmpty(column.Alias))
            _sql.Append(" AS ").AppendIdentifier(column.Alias);
        return this;
    }

    public IClickHouseSelect Sum(IColumn column)
    {
        IsFirst();

        _sql.Append("sum(").Append(column.ShortName).Append(')');
        if (!string.IsNullOrEmpty(column.Alias))
            _sql.Append(" AS ").AppendIdentifier(column.Alias);
        return this;
    }

    public IClickHouseSelect SumCount(IColumn column)
    {
        IsFirst();

        _sql.Append("sumCount(").Append(column.ShortName).Append(')');
        if (!string.IsNullOrEmpty(column.Alias))
            _sql.Append(" AS ").AppendIdentifier(column.Alias);
        return this;
    }

    public IClickHouseSelect SumIf(IColumn column, string conditionSql, string alias = "") =>
        SumIf(column.ShortName, conditionSql, alias);

    public IClickHouseSelect SumIf(string valueExpressionSql, string conditionSql, string alias = "") =>
        AppendIfAggregate("sumIf", valueExpressionSql, conditionSql, alias);

    public IClickHouseSelect AvgIf(IColumn column, string conditionSql, string alias = "") =>
        AvgIf(column.ShortName, conditionSql, alias);

    public IClickHouseSelect AvgIf(string valueExpressionSql, string conditionSql, string alias = "") =>
        AppendIfAggregate("avgIf", valueExpressionSql, conditionSql, alias);

    public IClickHouseSelect MinIf(IColumn column, string conditionSql, string alias = "") =>
        MinIf(column.ShortName, conditionSql, alias);

    public IClickHouseSelect MinIf(string valueExpressionSql, string conditionSql, string alias = "") =>
        AppendIfAggregate("minIf", valueExpressionSql, conditionSql, alias);

    public IClickHouseSelect MaxIf(IColumn column, string conditionSql, string alias = "") =>
        MaxIf(column.ShortName, conditionSql, alias);

    public IClickHouseSelect MaxIf(string valueExpressionSql, string conditionSql, string alias = "") =>
        AppendIfAggregate("maxIf", valueExpressionSql, conditionSql, alias);

    private IClickHouseSelect AppendIfAggregate(string functionName, string valueSql, string conditionSql, string alias)
    {
        IsFirst();

        _sql.Append(functionName).Append('(').Append(valueSql).Append(", ").Append(conditionSql).Append(')');
        if (!string.IsNullOrEmpty(alias))
            _sql.Append(" AS ").AppendIdentifier(alias);
        return this;
    }

    public IClickHouseSelect CountIf(string conditionSql, string alias = "")
    {
        IsFirst();

        _sql.Append("countIf(").Append(conditionSql).Append(')');
        if (!string.IsNullOrEmpty(alias))
            _sql.Append(" AS ").AppendIdentifier(alias);
        return this;
    }

    public IClickHouseSelect Min(IColumn column)
    {
        IsFirst();

        _sql.Append("min(").Append(column.ShortName).Append(')');

        if (!string.IsNullOrEmpty(column.Alias))
            _sql.Append(" AS ").AppendIdentifier(column.Alias);
            
        if (column is ClickHouseTuple && ((ClickHouseTuple)column).IsShowSeparatelyTupleItems)
        {
            _sql.Append(" ,").Append(column.Name);
        }

        return this;
    }

    public IClickHouseSelect Max(IColumn column)
    {
        IsFirst();

        _sql.Append("max(").Append(column.ShortName).Append(')');
        if (!string.IsNullOrEmpty(column.Alias))
            _sql.Append(" AS ").AppendIdentifier(column.Alias);
        return this;
    }


    public IClickHouseSelect AnyLast(IColumn column)
    {
        IsFirst();

        _sql.Append("anyLast(").Append(column.ShortName).Append(')');
        if (!string.IsNullOrEmpty(column.Alias))
            _sql.Append(" AS ").AppendIdentifier(column.Alias);
        return this;
    }

    public IClickHouseSelect AnyLastRespectNulls(IColumn column)
    {
        IsFirst();

        _sql.Append("anyLast(").Append(column.ShortName).Append(") RESPECT NULLS");
        if (!string.IsNullOrEmpty(column.Alias))
            _sql.Append(" AS ").AppendIdentifier(column.Alias);
        return this;
    }

    public IClickHouseSelect AnyHeavy(IColumn column)
    {
        IsFirst();

        _sql.Append("anyHeavy(").Append(column.ShortName).Append(')');
        if (!string.IsNullOrEmpty(column.Alias))
            _sql.Append(" AS ").AppendIdentifier(column.Alias);
        return this;
    }

    public IClickHouseSelect FirstValue(IColumn column)
    {
        IsFirst();

        _sql.Append("first_value(").Append(column.ShortName).Append(')');
        if (!string.IsNullOrEmpty(column.Alias))
            _sql.Append(" AS ").AppendIdentifier(column.Alias);
        return this;
    }

    public IClickHouseSelect ArgMax(IColumn value, IColumn by, string alias = "")
    {
        IsFirst();
        AppendBinaryAgg("argMax", value, by, alias);
        return this;
    }

    public IClickHouseSelect ArgMin(IColumn value, IColumn by, string alias = "")
    {
        IsFirst();
        AppendBinaryAgg("argMin", value, by, alias);
        return this;
    }

    public IClickHouseSelect ArgAndMax(IColumn value, IColumn by, string alias = "")
    {
        IsFirst();
        AppendBinaryAgg("argAndMax", value, by, alias);
        return this;
    }

    public IClickHouseSelect ArgAndMin(IColumn value, IColumn by, string alias = "")
    {
        IsFirst();
        AppendBinaryAgg("argAndMin", value, by, alias);
        return this;
    }

    private void AppendBinaryAgg(string name, IColumn value, IColumn by, string alias)
    {
        _sql.Append(name).Append('(').Append(value.ShortName).Append(", ").Append(by.ShortName).Append(')');
        if (!string.IsNullOrEmpty(alias))
            _sql.Append(" AS ").AppendIdentifier(alias);
    }

    public IClickHouseSelect Any(IColumn column)
    {
        IsFirst();

        _sql.Append("any(").Append(column.ShortName).Append(')');
        if (!string.IsNullOrEmpty(column.Alias))
            _sql.Append(" AS ").AppendIdentifier(column.Alias);
        return this;
    }

    public IClickHouseSelect AnyRespectNulls(IColumn column)
    {
        IsFirst();

        _sql.Append("any(").Append(column.ShortName).Append(") RESPECT NULLS");
        if (!string.IsNullOrEmpty(column.Alias))
            _sql.Append(" AS ").AppendIdentifier(column.Alias);
        return this;
    }

    public IClickHouseSelect ToUnixTimestamp(string dateTime, string alias)
    {
        _sql.Append("toUnixTimestamp('").Append(dateTime).Append("')");
        if (!string.IsNullOrEmpty(alias))
            _sql.Append(" AS ").AppendIdentifier(alias);
        return this;
    }


    public override string ToString()
    {
        var sql = _sql.Length == 0 ? "*" : _sql.ToString();
        return sql;
    }

    private void IsFirst()
    {
        if (!_isFirst)
            _sql.Append(", ");
        _isFirst = false;
    }

    public IClickHouseSelect Value(ClickHouseValue value, string alias)
    {
        IsFirst();

        _sql.Append(value);
        if (!string.IsNullOrEmpty(alias))
            _sql.Append(" AS ").AppendIdentifier(alias);

        return this;
    }

    public IClickHouseSelect Avg(IColumn column)
    {
        IsFirst();

        _sql.Append("avg(").Append(column.ShortName).Append(')');

        if (!string.IsNullOrEmpty(column.Alias))
            _sql.Append(" AS ").AppendIdentifier(column.Alias);

        if (column is ClickHouseTuple && ((ClickHouseTuple)column).IsShowSeparatelyTupleItems)
        {
            _sql.Append(" ,").Append(column.Name);
        }

        return this;
    }

    public IClickHouseSelect Function(IColumn column, string functionName, string alias)
    {
        IsFirst();
        _sql.Append(functionName).Append('(').Append(column.ShortName).Append(')');
        if (!string.IsNullOrEmpty(alias))
            _sql.Append(" AS ").AppendIdentifier(alias);
        return this;
    }

    public IClickHouseSelect FunctionOver(IColumn column, string funcName, Action<IWindowOver> configure, string alias = "")
    {
        var over = new WindowOverClause();
        configure(over);
        return FunctionOver(column, funcName, over.ToString(), alias);
    }

    public IClickHouseSelect FunctionOver(IColumn column, string funcName, string overClauseSql, string alias = "")
    {
        IsFirst();

        _sql.Append(funcName).Append('(').Append(column.ShortName).Append(") OVER (").Append(overClauseSql).Append(')');
        if (!string.IsNullOrEmpty(alias))
            _sql.Append(" AS ").AppendIdentifier(alias);
        return this;
    }

    public IClickHouseSelect RowNumberOver(Action<IWindowOver> configure, string alias = "")
    {
        var over = new WindowOverClause();
        configure(over);
        return RowNumberOver(over.ToString(), alias);
    }

    public IClickHouseSelect RowNumberOver(string overClauseSql, string alias = "")
    {
        IsFirst();

        _sql.Append("row_number() OVER (").Append(overClauseSql).Append(')');
        if (!string.IsNullOrEmpty(alias))
            _sql.Append(" AS ").AppendIdentifier(alias);
        return this;
    }

    public IClickHouseSelect CountAllOver(Action<IWindowOver> configure, string alias = "")
    {
        var over = new WindowOverClause();
        configure(over);
        return CountAllOver(over.ToString(), alias);
    }

    public IClickHouseSelect CountAllOver(string overClauseSql, string alias = "")
    {
        IsFirst();

        _sql.Append("COUNT(*) OVER (").Append(overClauseSql).Append(')');
        if (!string.IsNullOrEmpty(alias))
            _sql.Append(" AS ").AppendIdentifier(alias);
        return this;
    }
}