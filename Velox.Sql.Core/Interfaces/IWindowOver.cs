namespace Velox.Sql.Core.Interfaces;

/// <summary>
/// Contents of a window <c>OVER (...)</c> clause (PARTITION BY / ORDER BY).
/// </summary>
public interface IWindowOver
{
    IWindowOver PartitionBy(ITable table, string columnName);
    IWindowOver OrderByAsc(ITable table, string columnName);
    IWindowOver OrderByDesc(ITable table, string columnName);
}
