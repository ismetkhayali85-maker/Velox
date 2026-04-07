using System.Text;
using Velox.Sql.Core.Impl;
using Velox.Sql.Core.Interfaces;

namespace Velox.Sql.Core.ClickHouseSql.OrderBy;

public sealed class ClickHouseOrder
{
    private readonly StringBuilder _sql;
    private bool _isFirst;

    public ClickHouseOrder()
    {
        _sql = new StringBuilder();
        _isFirst = true;
    }

    public override string ToString()
    {
        var result = _sql.ToString();
        return result;
    }

    public ClickHouseOrder Asc(IColumn column)
    {
        IsFirst();
        _sql.Append(column.ShortName)
            .Append(SqlHelper.Asc);
        return this;
    }

    public ClickHouseOrder Desc(IColumn column)
    {
        IsFirst();
        _sql.Append(column.ShortName)
            .Append(SqlHelper.Desc);
        return this;
    }

    private void IsFirst()
    {
        if (_isFirst)
            _isFirst = false;
        else
            _sql.Append(", ");
    }
}