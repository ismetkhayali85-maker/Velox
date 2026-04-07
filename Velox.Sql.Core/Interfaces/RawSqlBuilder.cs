using System;
using System.Collections.Generic;

namespace Velox.Sql.Core.Interfaces;

public class RawSqlBuilder : ISqlBuilder<RawSqlBuilder>
{
    private readonly string _sql;

    public RawSqlBuilder(string sql)
    {
        _sql = sql;
    }

    public string Build() => _sql;
    public string BuildWithoutEnd() => _sql;

    public RawSqlBuilder AddSql(string sql) => throw new NotImplementedException();
    public RawSqlBuilder Comments(string commentText) => throw new NotImplementedException();
    public RawSqlBuilder Select() => throw new NotImplementedException();
    public RawSqlBuilder Select(Action<ISelect> action) => throw new NotImplementedException();
    public RawSqlBuilder SelectInto(ITable whichITable, Action<ISelect> action) => throw new NotImplementedException();
    public RawSqlBuilder Delete() => throw new NotImplementedException();
    public RawSqlBuilder Truncate(ITable table) => throw new NotImplementedException();
    public RawSqlBuilder From(ITable table) => throw new NotImplementedException();
    public RawSqlBuilder From(List<ITable> tables) => throw new NotImplementedException();
    public RawSqlBuilder Where(Action<IWhere> action) => throw new NotImplementedException();
    public RawSqlBuilder OrderBy(Action<IOrder> orders) => throw new NotImplementedException();
    public RawSqlBuilder Offset(ulong value) => throw new NotImplementedException();
    public RawSqlBuilder Join<T>(IJoin<T> value) => throw new NotImplementedException();
    public RawSqlBuilder Insert(ITable table, Action<IInsert> action) => throw new NotImplementedException();
    public RawSqlBuilder Insert(ITable table) => throw new NotImplementedException();
    public RawSqlBuilder Update(ITable table, Action<IUpdate> action) => throw new NotImplementedException();
    public RawSqlBuilder GroupBy(Action<IGroupBy> items) => throw new NotImplementedException();
    public RawSqlBuilder Having(Action<IHaving> columns) => throw new NotImplementedException();
}
