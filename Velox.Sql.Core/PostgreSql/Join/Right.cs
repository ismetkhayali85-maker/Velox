using Velox.Sql.Core.Impl;
using Velox.Sql.Core.Interfaces;
using Velox.Sql.Core.Interfaces.Join;

namespace Velox.Sql.Core.PostgreSql.Join;

public sealed class Right : Base, IJoinExpression<Right>
{
    private readonly string _tablePrefix;

    public Right(ITable table)
    {
        _tablePrefix = table.ToString();
        Sql.Append(" RIGHT JOIN ").Append(table.Init());
    }

    public IJoinCondition<Right> On(string fromField, ITable fromTable, string toField)
    {
        Sql.Append(" ON ")
            .Append(_tablePrefix)
            .Append('.')
            .AppendIdentifier(fromField)
            .Append(" = ")
            .AppendTable(fromTable)
            .Append('.')
            .AppendIdentifier(toField);
        return new Condition<Right>(this, _tablePrefix);
    }

    public IJoin<Right> Using(string fieldsName)
    {
        Sql.Append(" USING (").AppendIdentifier(fieldsName).Append(')');
        return this;
    }
}