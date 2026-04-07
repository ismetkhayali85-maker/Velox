using System.Text;
using Velox.Sql.Core.Impl;
using Velox.Sql.Core.Interfaces;

namespace Velox.Sql.Core.PostgreSql;

public sealed class Function : IFunction
{
    private readonly string _short;
    private readonly StringBuilder _sql;
    private bool _isFirst;

    public Function(string name)
    {
        _isFirst = true;
        _sql = new StringBuilder();

        Name = name;
        _short = $"\"{Name.Replace("\"", "\"\"")}\"";
    }

    public string Name { get; }

    public void SetValue(string name, IValue value)
    {
        IsFirst();
        _sql.AppendIdentifier(name)
            .Append(" := ")
            .Append(value);
    }

    public override string ToString()
    {
        return new StringBuilder(_short)
            .Append('(')
            .Append(_sql)
            .Append(')')
            .ToString();
    }

    private void IsFirst()
    {
        if (_isFirst)
            _isFirst = false;
        else
            _sql.Append(", ");
    }
}