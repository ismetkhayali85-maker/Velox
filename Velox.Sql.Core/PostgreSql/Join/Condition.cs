using Velox.Sql.Core.Impl;
using Velox.Sql.Core.Interfaces;
using Velox.Sql.Core.Interfaces.Join;

namespace Velox.Sql.Core.PostgreSql.Join;

public sealed class Condition<T> : IJoinCondition<T> where T : Base
{
    private readonly string _tablePrefix;
    private readonly T _parent;

    public Condition(T parent, string tablePrefix)
    {
        _parent = parent;
        _tablePrefix = tablePrefix;
    }

    public IJoinCondition<T> And(string tableField, IValue value)
    {
        _parent.Sql.Append(" AND ")
            .Append(_tablePrefix)
            .Append('.')
            .AppendIdentifier(tableField)
            .Append(" = ")
            .Append(value);
        return this;
    }

    public override string ToString()
    {
        return _parent.ToString();
    }
}