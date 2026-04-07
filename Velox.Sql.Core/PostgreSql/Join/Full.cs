using Velox.Sql.Core.Impl;
using Velox.Sql.Core.Interfaces;
using Velox.Sql.Core.Interfaces.Join;

namespace Velox.Sql.Core.PostgreSql.Join;

public sealed class Full : Base, IJoinExpression<Full>
{
    private readonly string _tablePrefix;

    public Full(ITable table)
    {
        _tablePrefix = table.ToString();
        Sql.Append(" FULL JOIN ").Append(table.Init());
    }

    public IJoinCondition<Full> On(string fromField, ITable fromTable, string toField)
    {
        Sql.Append(" ON ")
            .Append(_tablePrefix)
            .Append('.')
            .AppendIdentifier(fromField)
            .Append(" = ")
            .AppendTable(fromTable)
            .Append('.')
            .AppendIdentifier(toField);
        return new Condition<Full>(this, _tablePrefix);
    }

    public IJoin<Full> Using(string fieldsName)
    {
        Sql.Append(" USING (").AppendIdentifier(fieldsName).Append(')');
        return this;
    }
}