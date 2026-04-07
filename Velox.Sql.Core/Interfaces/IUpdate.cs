namespace Velox.Sql.Core.Interfaces;

public interface IUpdate
{
    IUpdate SetValue(string name, IValue value);
    IUpdate SetValue<TBuilder>(string name, ISqlBuilder<TBuilder> builder);
}