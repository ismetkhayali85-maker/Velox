using System;
using System.Collections.Generic;
using System.Text;
using Velox.Sql.Core.Interfaces;
using Velox.Sql.Core.PostgreSql.GroupBy;
using Velox.Sql.Core.PostgreSql.Having;
using Velox.Sql.Core.PostgreSql.Insert;
using Velox.Sql.Core.PostgreSql.OrderBy;
using Velox.Sql.Core.PostgreSql.Returning;
using Velox.Sql.Core.PostgreSql.Select;
using Velox.Sql.Core.PostgreSql.Update;
using Velox.Sql.Core.PostgreSql.Where;

namespace Velox.Sql.Core.PostgreSql;

public sealed class PostgreSqlBuilder : ISqlBuilder<PostgreSqlBuilder>
{
    private readonly StringBuilder _sqlAppender;

    public PostgreSqlBuilder()
    {
        _sqlAppender = new StringBuilder();
    }

    public PostgreSqlBuilder AddSql(string sql)
    {
        _sqlAppender.Append(sql);
        return this;
    }

    public PostgreSqlBuilder Comments(string commentText)
    {
        _sqlAppender.Append("/* ")
            .Append(commentText)
            .Append(" */");
        return this;
    }

    public PostgreSqlBuilder Select()
    {
        _sqlAppender.Append("SELECT *");
        return this;
    }

    public PostgreSqlBuilder Select(Action<ISelect> action)
    {
        if (action == null)
        {
            Select();
            return this;
        }

        ISelect columns = new Columns();
        action(columns);
        _sqlAppender.Append("SELECT ")
            .Append(columns);
        return this;
    }

    public PostgreSqlBuilder SelectInto(ITable whichTable, Action<ISelect> action)
    {
        ISelect columns = new Columns();
        action(columns);
        _sqlAppender.Append("SELECT ")
            .Append(columns)
            .Append(" INTO ")
            .Append(whichTable);
        return this;
    }

    public PostgreSqlBuilder Update(ITable table, Action<IUpdate> action)
    {
        var set = new Set();
        action(set);

        _sqlAppender.Append("UPDATE ")
            .Append(table)
            .Append(" SET ")
            .Append(set);
        return this;
    }

    public PostgreSqlBuilder Join<T>(IJoin<T> value)
    {
        _sqlAppender.Append(value);
        return this;
    }

    public PostgreSqlBuilder Join(string sql)
    {
        _sqlAppender.Append(sql);
        return this;
    }


    public PostgreSqlBuilder Insert(ITable table, Action<IInsert> action)
    {
        var inserter = new Inserter();
        action(inserter);
        _sqlAppender.Append("INSERT INTO ")
            .Append(table)
            .Append(inserter);
        return this;
    }

    public PostgreSqlBuilder Insert(ITable table)
    {
        _sqlAppender.Append("INSERT INTO ")
            .Append(table)
            .Append(" DEFAULT VALUES");
        return this;
    }

    public PostgreSqlBuilder Delete()
    {
        _sqlAppender.Append("DELETE");
        return this;
    }

    public PostgreSqlBuilder Truncate(ITable table)
    {
        _sqlAppender.Append("TRUNCATE TABLE ")
            .Append(table);
        return this;
    }

    public PostgreSqlBuilder From(ITable table)
    {
        _sqlAppender.Append(" FROM ");
        if (!string.IsNullOrEmpty(table.Alias))
            _sqlAppender.Append(table.Init());
        else
            _sqlAppender.Append(table);

        return this;
    }

    public PostgreSqlBuilder FromFunc(IFunction func, Action<IFunction> action)
    {
        var function = new Function(func.Name);
        action(function);

        _sqlAppender.Append(" FROM ")
            .Append(function);
        return this;
    }

    public PostgreSqlBuilder Offset(ulong value)
    {
        _sqlAppender.Append(" OFFSET ")
            .Append(value);
        return this;
    }

    public PostgreSqlBuilder OrderBy(Action<IOrder> action)
    {
        var orderColumn = new Order();
        action(orderColumn);
        _sqlAppender.Append(" ORDER BY ")
            .Append(orderColumn);
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

    public PostgreSqlBuilder Where(Action<IWhere> action)
    {
        var predicate = new Predicate();
        action(predicate);

        _sqlAppender.Append(" WHERE ")
            .Append(predicate.ToSql());
        return this;
    }

    public PostgreSqlBuilder Where(string sql)
    {
        _sqlAppender.Append(" WHERE ")
            .Append(sql);
        return this;
    }

    public PostgreSqlBuilder GroupBy(Action<IGroupBy> items)
    {
        var groupItems = new Items();
        items(groupItems);
        _sqlAppender.Append(" GROUP BY ")
            .Append(groupItems);
        return this;
    }

    public PostgreSqlBuilder Having(Action<IHaving> columns)
    {
        var havingColumn = new HavingColumn();
        columns(havingColumn);
        _sqlAppender.Append(" HAVING ")
            .Append(havingColumn);
        return this;
    }

    public PostgreSqlBuilder From(List<ITable> tables)
    {
        _sqlAppender.Append(" FROM ");
        _sqlAppender.Append(string.Join(", ", tables));

        return this;
    }

    public PostgreSqlBuilder Returning(Action<IReturning> action)
    {
        var returningColumn = new ReturningColumn();
        action(returningColumn);
        _sqlAppender.Append(" RETURNING ")
            .Append(returningColumn);
        return this;
    }

    public PostgreSqlBuilder UnionAll()
    {
        _sqlAppender.Append(" UNION ALL ");
        return this;
    }

    public PostgreSqlBuilder IntersectAll(bool isAll = false)
    {
        _sqlAppender.Append(isAll ? " INTERSECT ALL " : " INTERSECT ");
        return this;
    }

    public PostgreSqlBuilder ExceptAll(bool isAll = false)
    {
        _sqlAppender.Append(isAll ? " EXCEPT ALL " : " EXCEPT ");
        return this;
    }

    public PostgreSqlBuilder Intersect()
    {
        _sqlAppender.Append(" INTERSECT ");
        return this;
    }

    public PostgreSqlBuilder Except()
    {
        _sqlAppender.Append(" EXCEPT ");
        return this;
    }

    public PostgreSqlBuilder Union()
    {
        _sqlAppender.Append(" UNION ");
        return this;
    }

    public PostgreSqlBuilder IntersectAll()
    {
        _sqlAppender.Append(" INTERSECT ALL ");
        return this;
    }

    public PostgreSqlBuilder ExceptAll()
    {
        _sqlAppender.Append(" EXCEPT ALL ");
        return this;
    }

    public PostgreSqlBuilder Limit(ulong value)
    {
        _sqlAppender.Append(" LIMIT ")
            .Append(value);
        return this;
    }

    public PostgreSqlBuilder Limit()
    {
        _sqlAppender.Append(" LIMIT ALL");
        return this;
    }
}