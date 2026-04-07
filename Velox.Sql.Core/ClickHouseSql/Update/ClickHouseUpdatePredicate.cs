using System.Text;
using Velox.Sql.Core.Impl;
using Velox.Sql.Core.Interfaces;

namespace Velox.Sql.Core.ClickHouseSql.Update;

public sealed class ClickHouseUpdatePredicate// : IUpdate
{
    private readonly StringBuilder _sql;
    private bool _isFirst;

    public ClickHouseUpdatePredicate()
    {
        _isFirst = true;
        _sql = new StringBuilder();
    }

    public ClickHouseUpdatePredicate SetValue(string name, ClickHouseValue value)
    {
        IsFirst();
        _sql.AppendIdentifier(name)
            .Append(" = ")
            .Append(value);
        return this;
    }

    public IUpdate SetValue<TBuilder>(string name, ISqlBuilder<TBuilder> builder)
    {
        throw new System.NotImplementedException();
    }

    public override string ToString()
    {
        return _sql.ToString();
    }

    private void IsFirst()
    {
        if (_isFirst)
            _isFirst = false;
        else
            _sql.Append(", ");
    }
}