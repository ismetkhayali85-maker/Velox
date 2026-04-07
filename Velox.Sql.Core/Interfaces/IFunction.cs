namespace Velox.Sql.Core.Interfaces;

public interface IFunction
{
    string Name { get; }
    void SetValue(string name, IValue value);
}