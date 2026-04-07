using Velox.Sql.Core.Interfaces;

namespace Velox.Sql.Core.ClickHouseSql.Select;

public interface IClickHouseSelect
{
    IClickHouseSelect CountAll(string alias);
    IClickHouseSelect Count(IColumn column);
    IClickHouseSelect Distinct(IColumn column);
    IClickHouseSelect Sum(IColumn column);
    IClickHouseSelect AnyLast(IColumn column);
    IClickHouseSelect Any(IColumn column);
    IClickHouseSelect Column(IColumn column);
    IClickHouseSelect Value(ClickHouseValue value, string alias);
    IClickHouseSelect DistinctCount(IColumn column);
    IClickHouseSelect ToUnixTimestamp(string dateTime, string alias);
    IClickHouseSelect Min(IColumn column);
    IClickHouseSelect Max(IColumn column);
    IClickHouseSelect Avg(IColumn column);
    IClickHouseSelect Function(IColumn column, string functionName, string alias);
}