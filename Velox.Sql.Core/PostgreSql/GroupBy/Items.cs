using System;
using System.Text;
using Velox.Sql.Core.Impl;
using Velox.Sql.Core.Interfaces;

namespace Velox.Sql.Core.PostgreSql.GroupBy;

public sealed class Items : IGroupBy
{
    private readonly StringBuilder _sql;
    private bool _isFirst;

    public Items()
    {
        _sql = new StringBuilder();
        _isFirst = true;
    }

    public IGroupBy Item(ITable table, string columnName)
    {
        if (_isFirst)
            _isFirst = false;
        else
            _sql.Append(", ");

        _sql.AppendTable(table)
            .Append('.')
            .AppendIdentifier(columnName);
        return this;
    }

    public override string ToString()
    {
        return _sql.ToString();
    }

    public IGroupBy GroupingSets(Action<IGroupBy> columns)
    {
        var rollItem = new Items();
        columns(rollItem);
        _sql.Append("GROUPING SETS (")
            .Append(rollItem)
            .Append(')');
        return this;
    }

    public IGroupBy Cube(Action<IGroupBy> columns)
    {
        var rollItem = new Items();
        columns(rollItem);
        _sql.Append("CUBE (")
            .Append(rollItem)
            .Append(')');
        return this;
    }

    public IGroupBy Rollup(Action<IGroupBy> columns)
    {
        var rollItem = new Items();
        columns(rollItem);
        _sql.Append("ROLLUP (")
            .Append(rollItem)
            .Append(')');
        return this;
    }
}