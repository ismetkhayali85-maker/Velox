using System.Text;
using Velox.Sql.Core.Interfaces;

namespace Velox.Sql.Core.Impl;

public static class SqlExtensions
{
    /// <summary>
    /// Appends a quoted identifier to the StringBuilder.
    /// Handles double-quote escaping.
    /// </summary>
    public static StringBuilder AppendIdentifier(this StringBuilder sb, string name)
    {
        if (string.IsNullOrEmpty(name)) return sb;
        return sb.Append('"').Append(name.Replace("\"", "\"\"")).Append('"');
    }

    /// <summary>
    /// Appends a table identifier (schema.table or just table).
    /// If an alias is present, it uses the alias as per ToString() implementation.
    /// </summary>
    public static StringBuilder AppendTable(this StringBuilder sb, ITable table)
    {
        if (table == null) return sb;
        return sb.Append(table.ToString());
    }

    /// <summary>
    /// Appends a table and column identifier: table."column"
    /// </summary>
    public static StringBuilder AppendColumn(this StringBuilder sb, ITable table, string columnName)
    {
        if (table != null)
        {
            sb.Append(table).Append('.');
        }

        return sb.AppendIdentifier(columnName);
    }
}
