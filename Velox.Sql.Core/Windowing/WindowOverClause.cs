using System.Text;
using Velox.Sql.Core.Impl;
using Velox.Sql.Core.Interfaces;

namespace Velox.Sql.Core.Windowing;

/// <summary>
/// Builds the inside of <c>OVER (...)</c> (PostgreSQL and ClickHouse-compatible).
/// </summary>
public sealed class WindowOverClause : IWindowOver
{
    private readonly StringBuilder _partition = new();
    private readonly StringBuilder _order = new();
    private bool _firstPartition = true;
    private bool _firstOrder = true;

    public IWindowOver PartitionBy(ITable table, string columnName)
    {
        if (!_firstPartition)
            _partition.Append(", ");
        _firstPartition = false;
        _partition.AppendTable(table)
            .Append('.')
            .AppendIdentifier(columnName);
        return this;
    }

    public IWindowOver OrderByAsc(ITable table, string columnName)
    {
        if (!_firstOrder)
            _order.Append(", ");
        _firstOrder = false;
        _order.AppendTable(table)
            .Append('.')
            .AppendIdentifier(columnName)
            .Append(SqlHelper.Asc);
        return this;
    }

    public IWindowOver OrderByDesc(ITable table, string columnName)
    {
        if (!_firstOrder)
            _order.Append(", ");
        _firstOrder = false;
        _order.AppendTable(table)
            .Append('.')
            .AppendIdentifier(columnName)
            .Append(SqlHelper.Desc);
        return this;
    }

    public override string ToString()
    {
        var sb = new StringBuilder();
        if (_partition.Length > 0)
            sb.Append("PARTITION BY ").Append(_partition);
        if (_order.Length > 0)
        {
            if (sb.Length > 0)
                sb.Append(' ');
            sb.Append("ORDER BY ").Append(_order);
        }

        return sb.ToString();
    }
}
