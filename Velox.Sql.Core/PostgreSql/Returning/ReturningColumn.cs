using System.Collections.Generic;
using Velox.Sql.Core.Interfaces;

namespace Velox.Sql.Core.PostgreSql.Returning;

public sealed class ReturningColumn : IReturning
{
    private string _sql;
    public string SetValue(IColumn returnItem)
    {
        if (string.IsNullOrEmpty(_sql))
            _sql = returnItem.ToString();
        else
            _sql += ", " + returnItem;
        return _sql;
    }

    public string SetValue(List<IColumn> returnItems)
    {
        _sql = string.Join(", ", returnItems);
        return _sql;
    }

    public string All()
    {
        _sql = "*";
        return _sql;
    }

    public override string ToString()
    {
        return _sql;
    }
}