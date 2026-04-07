using Velox.Sql.Core.Impl;
using Velox.Sql.Core.Interfaces;
using Velox.Sql.Core.Interfaces.Join;

namespace Velox.Sql.Core.PostgreSql.Join;

public sealed class Left : Base, IJoinExpression<Left>
{
    private readonly string _tablePrefix;

    public Left(ITable table)
    {
        _tablePrefix = table.ToString();
        Sql.Append(" LEFT JOIN ").Append(table.Init());
    }

    public IJoinCondition<Left> On(string fromField, ITable fromTable, string toField)
    {
        Sql.Append(" ON ")
            .Append(_tablePrefix)
            .Append('.')
            .AppendIdentifier(fromField)
            .Append(" = ")
            .AppendTable(fromTable)
            .Append('.')
            .AppendIdentifier(toField);
        return new Condition<Left>(this, _tablePrefix);
    }

    public IJoin<Left> Using(string fieldsName)
    {
        Sql.Append(" USING (").AppendIdentifier(fieldsName).Append(')');
        return this;
    }
}