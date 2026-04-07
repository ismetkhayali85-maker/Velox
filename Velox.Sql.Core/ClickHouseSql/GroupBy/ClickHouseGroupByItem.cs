using System.Text;
using Velox.Sql.Core.Impl;
using Velox.Sql.Core.Interfaces;

namespace Velox.Sql.Core.ClickHouseSql.GroupBy;

public sealed class ClickHouseGroupByItem
{
    private readonly StringBuilder _sql;
    private bool _isFirst;

    public ClickHouseGroupByItem()
    {
        _sql = new StringBuilder();
        _isFirst = true;
    }

    public override string ToString()
    {
        return _sql.ToString();
    }

    public ClickHouseGroupByItem Item(string columnName)
    {
        return Item(null, columnName);
    }

    public ClickHouseGroupByItem Item(ITable table, string columnName)
    {
        if (!_isFirst)
            _sql.Append(", ");

        _isFirst = false;

        _sql.AppendTable(table);
        if (table != null) _sql.Append('.');

        _sql.AppendIdentifier(columnName);

        return this;
    }
}