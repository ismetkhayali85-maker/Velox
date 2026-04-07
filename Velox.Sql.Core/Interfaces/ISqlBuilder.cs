using System;
using System.Collections.Generic;

namespace Velox.Sql.Core.Interfaces;

public interface ISqlBuilder<TBuilder>
{
    TBuilder AddSql(string sql);
    TBuilder Comments(string commentText);
    TBuilder Select();
    TBuilder Select(Action<ISelect> action);
    TBuilder SelectInto(ITable whichITable, Action<ISelect> action);
    TBuilder Delete();
    TBuilder Truncate(ITable table);
    TBuilder From(ITable table);
    TBuilder From(List<ITable> tables);
    TBuilder Where(Action<IWhere> action);
    TBuilder OrderBy(Action<IOrder> orders);
    TBuilder Offset(ulong value);
    TBuilder Join<T>(IJoin<T> value);
    TBuilder Insert(ITable table, Action<IInsert> action);
    TBuilder Insert(ITable table);
    TBuilder Update(ITable table, Action<IUpdate> action);
    TBuilder GroupBy(Action<IGroupBy> items);
    TBuilder Having(Action<IHaving> columns);
    string Build();
    string BuildWithoutEnd();
}