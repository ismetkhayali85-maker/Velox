using System.Collections.Generic;

namespace Velox.Sql.Core.Impl;

public sealed class SqlQuery
{
    public string Sql { get; set; } = string.Empty;
    public Dictionary<string, object> Parameters { get; set; } = new();

    /// <summary>
    /// Implicitly converts the SqlQuery to a string. Returns only the SQL part.
    /// </summary>
    public static implicit operator string(SqlQuery query) => query?.Sql;

    public override string ToString() => Sql;
}
