namespace Velox.Sql.Core.Interfaces;

public interface IOrder
{
    IOrder Asc(ITable table, string columnName);
    IOrder Desc(ITable table, string columnName);
    IOrder AscByAlias(string aliasName);
    IOrder DescByAlias(string aliasName);
}