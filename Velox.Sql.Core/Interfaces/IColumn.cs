namespace Velox.Sql.Core.Interfaces;

public interface IColumn : ISqlConvertable<IColumn>
{
    string Name { get; }
    string ShortName { get; }
    string Alias { get; }
}