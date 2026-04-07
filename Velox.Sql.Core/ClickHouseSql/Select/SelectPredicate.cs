using System.Text;
using Velox.Sql.Core.Impl;
using Velox.Sql.Core.Interfaces;

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

    public IClickHouseSelect Any(IColumn column)
    {
        IsFirst();

        _sql.Append("any (").Append(column.ShortName).Append(')');
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
}