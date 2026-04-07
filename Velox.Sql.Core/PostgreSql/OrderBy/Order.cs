using System.Text;
using Velox.Sql.Core.Impl;
using Velox.Sql.Core.Interfaces;

namespace Velox.Sql.Core.PostgreSql.OrderBy;

public sealed class Order : IOrder
{
    private readonly StringBuilder _sql;
    private bool _isFirst;

    public Order()
    {
        _sql = new StringBuilder();
        _isFirst = true;
    }

    public override string ToString()
    {
        var result = _sql.ToString();
        _sql.Clear();
        return result;
    }

    public IOrder Asc(ITable table, string columnName)
    {
        IsFirst();
        _sql.AppendTable(table)
            .Append('.')
            .AppendIdentifier(columnName)
            .Append(SqlHelper.Asc);
        return this;
    }

    public IOrder Desc(ITable table, string columnName)
    {
        IsFirst();
        _sql.AppendTable(table)
            .Append('.')
            .AppendIdentifier(columnName)
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

    public IOrder AscByAlias(string aliasName)
    {
        IsFirst();
        _sql.AppendIdentifier(aliasName)
            .Append(SqlHelper.Asc);
        return this;
    }

    public IOrder DescByAlias(string aliasName)
    {
        IsFirst();
        _sql.AppendIdentifier(aliasName)
            .Append(SqlHelper.Desc);
        return this;
    }
}