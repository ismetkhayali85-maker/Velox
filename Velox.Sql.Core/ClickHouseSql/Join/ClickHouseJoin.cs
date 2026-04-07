using System.Text;
using Velox.Sql.Core.Impl;
using Velox.Sql.Core.Interfaces;

namespace Velox.Sql.Core.ClickHouseSql.Join;

public sealed class ClickHouseJoin
{
    private readonly StringBuilder _sql;

    public ClickHouseJoin(string joinType, ITable table)
    {
        _sql = new StringBuilder();
        _sql.Append(' ').Append(joinType).Append(" JOIN ").Append(table.Init());
    }

    public ClickHouseJoin On(string fromField, ITable fromTable, string toField, ITable toTable)
    {
        _sql.Append(" ON ")
            .AppendTable(fromTable)
            .Append('.')
            .AppendIdentifier(fromField)
            .Append(" = ")
            .AppendTable(toTable)
            .Append('.')
            .AppendIdentifier(toField);
        return this;
    }

    public override string ToString()
    {
        return _sql.ToString();
    }
}
