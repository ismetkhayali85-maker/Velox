using Velox.Sql.Core.Interfaces;

namespace Velox.Sql.Core.PostgreSql.Join;

public sealed class Cross : Base, IJoin<Cross>
{
    public Cross(ITable table)
    {
        Sql.Append(" CROSS JOIN ").Append(table.Init());
    }

    public string ToSql()
    {
        return Sql.ToString();
    }
}