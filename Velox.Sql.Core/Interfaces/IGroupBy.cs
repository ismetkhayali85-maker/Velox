using System;

namespace Velox.Sql.Core.Interfaces;

public interface IGroupBy
{
    IGroupBy Item(ITable table, string columnName);
    IGroupBy GroupingSets(Action<IGroupBy> columns);
    IGroupBy Cube(Action<IGroupBy> columns);
    IGroupBy Rollup(Action<IGroupBy> columns);
}