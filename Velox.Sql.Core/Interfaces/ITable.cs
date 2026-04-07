namespace Velox.Sql.Core.Interfaces;

public interface ITable
{
    string Init();
    string Schema { get; }
    string Name { get; }
    string Alias { get; }
}