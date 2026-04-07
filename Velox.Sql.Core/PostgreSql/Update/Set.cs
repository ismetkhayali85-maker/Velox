using System.Text;
using Velox.Sql.Core.Impl;
using Velox.Sql.Core.Interfaces;

namespace Velox.Sql.Core.PostgreSql.Update;

public sealed class Set : IUpdate
{
    private readonly StringBuilder _sql;
    private bool _isFirst;

    public Set()
    {
        _isFirst = true;
        _sql = new StringBuilder();
    }

    public IUpdate SetValue(string name, IValue value)
    {
        IsFirst();
        _sql.AppendIdentifier(name)
            .Append(" = ")
            .Append(value);
        return this;
    }

    public IUpdate SetValue<PostgreSqlBuilder>(string name, ISqlBuilder<PostgreSqlBuilder> builder)
    {
        IsFirst();
        _sql.AppendIdentifier(name)
            .Append(" = (")
            .Append(builder.BuildWithoutEnd())
            .Append(") ");
        return this;
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