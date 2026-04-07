 
using System;
using System.Text;
using Velox.Sql.Core.Interfaces;
using Velox.Sql.Core.ClickHouseSql.Select;
using Velox.Sql.Core.ClickHouseSql.Insert;
using Velox.Sql.Core.ClickHouseSql.Update;
using Velox.Sql.Core.ClickHouseSql.Where;
using Velox.Sql.Core.ClickHouseSql.Having;
using Velox.Sql.Core.ClickHouseSql.Join;
using Velox.Sql.Core.ClickHouseSql.GroupBy;
using Velox.Sql.Core.ClickHouseSql.OrderBy;


namespace Velox.Sql.Core.ClickHouseSql;

public sealed class ClickHouseSqlBuilder //: IClickHouseSqlBuilder
{
    private readonly StringBuilder _sqlAppender;

    public ClickHouseSqlBuilder()
    {
        _sqlAppender = new StringBuilder();
    }

    public ClickHouseSqlBuilder AddSql(string sql)
    {
        _sqlAppender.Append(sql);
        return this;
    }

    public ClickHouseSqlBuilder GroupBy(Action<ClickHouseGroupByItem> action)
    {
        var groupItems = new ClickHouseGroupByItem();
        action(groupItems);
        _sqlAppender.Append(" GROUP BY ")
            .Append(groupItems);
        return this;
    }

    public ClickHouseSqlBuilder Select()
    {
        _sqlAppender.Append("SELECT *");
        return this;
    }

    public ClickHouseSqlBuilder Select(Action<IClickHouseSelect> action)
    {
        if (action == null)
        {
            Select();
            return this;
        }

        IClickHouseSelect selectPredicate = new SelectPredicate();
        action(selectPredicate);
        _sqlAppender.Append("SELECT ")
            .Append(selectPredicate);
        return this;
    }

    public ClickHouseSqlBuilder From(ITable table)
    {
        _sqlAppender.Append(" FROM ");
        if (!string.IsNullOrEmpty(table.Alias))
            _sqlAppender.Append(table.Init());
        else
            _sqlAppender.Append(table);

        return this;
    }

    public ClickHouseSqlBuilder Join(string sql)
    {
        _sqlAppender.Append(sql);
        return this;
    }

    public ClickHouseSqlBuilder Final()
    {
        _sqlAppender.Append(" FINAL ");
        return this;
    }

    public ClickHouseSqlBuilder Limit(ulong value)
    {
        _sqlAppender.Append(" LIMIT ")
            .Append(value);
        return this;
    }

    public ClickHouseSqlBuilder Offset(ulong value)
    {
        _sqlAppender.Append(" OFFSET ")
            .Append(value);
        return this;
    }

    public ClickHouseSqlBuilder OrderBy(Action<ClickHouseOrder> action)
    {
        var orderColumn = new ClickHouseOrder();
        action(orderColumn);
        _sqlAppender.Append(" ORDER BY ")
            .Append(orderColumn);
        return this;
    }

    public ClickHouseSqlBuilder Insert(ITable table, Action<ClickHouseInsertPredicate> action)
    {
        var inserter = new ClickHouseInsertPredicate();
        action(inserter);
        _sqlAppender.Append("INSERT INTO ")
            .Append(table)
            .Append(inserter);
        return this;
    }

    public ClickHouseSqlBuilder Where(Action<ClickHouseWherePredicate> action)
    {
        var predicate = new ClickHouseWherePredicate();
        action(predicate);

        _sqlAppender.Append(" WHERE ")
            .Append(predicate.ToSql());
        return this;
    }

    public ClickHouseSqlBuilder Where(string sql)
    {
        _sqlAppender.Append(" WHERE ")
            .Append(sql);
        return this;
    }

    public ClickHouseSqlBuilder Update(ITable table, Action<ClickHouseUpdatePredicate> action)
    {
        var set = new ClickHouseUpdatePredicate();
        action(set);

        _sqlAppender.Append("ALTER TABLE ")
            .Append(table)
            .Append(" UPDATE ")
            .Append(set);
        return this;
    }

    public ClickHouseSqlBuilder Delete(ITable table, Action<ClickHouseWherePredicate> whereAction)
    {
        var wherePredicate = new ClickHouseWherePredicate();
        whereAction(wherePredicate);

        _sqlAppender.Append("ALTER TABLE ")
            .Append(table)
            .Append(" DELETE WHERE ")
            .Append(wherePredicate.ToSql());
        return this;
    }

    public ClickHouseSqlBuilder Having(Action<IHaving> action)
    {
        var having = new ClickHouseHaving();
        action(having);
        _sqlAppender.Append(" HAVING ")
            .Append(having);
        return this;
    }


    public string Build()
    {
        var sql = _sqlAppender.Append(";").ToString();
        _sqlAppender.Clear();
        return sql;
    }

    public string BuildWithoutEnd()
    {
        var sql = _sqlAppender.ToString();
        _sqlAppender.Clear();
        return sql;
    }
}