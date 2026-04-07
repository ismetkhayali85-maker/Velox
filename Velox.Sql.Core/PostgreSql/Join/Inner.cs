using Velox.Sql.Core.Impl;
using Velox.Sql.Core.Interfaces;
using Velox.Sql.Core.Interfaces.Join;

namespace Velox.Sql.Core.PostgreSql.Join;

public sealed class Inner : Base, IJoinExpression<Inner>
{
    private readonly string _tablePrefix;

    public Inner(ITable table)
    {
        _tablePrefix = table.ToString();
        Sql.Append(" INNER JOIN ").Append(table.Init());
    }

    public IJoinCondition<Inner> On(string fromField, ITable fromTable, string toField)
    {
        Sql.Append(" ON ")
            .AppendTable(fromTable)
            .Append('.')
            .AppendIdentifier(fromField)
            .Append(" = ")
            .Append(_tablePrefix)
            .Append('.')
            .AppendIdentifier(toField);
        return new Condition<Inner>(this, _tablePrefix);
    }

    public IJoin<Inner> Using(string fieldsName)
    {
        Sql.Append(" USING (").AppendIdentifier(fieldsName).Append(')');
        return this;
    }
}