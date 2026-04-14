using System;
using System.Text;
using Velox.Sql.Core.Impl;
using Velox.Sql.Core.Interfaces;
using Velox.Sql.Core.Windowing;

namespace Velox.Sql.Core.PostgreSql.Select;

public sealed class Columns : ISelect
{
    private readonly StringBuilder _sql;
    private bool _isFirst;

    public Columns()
    {
        _sql = new StringBuilder();
        _isFirst = true;
    }

    public ISelect Column(IColumn column)
    {
        IsFirst();

        _sql.Append(column);
        return this;
    }

    public ISelect CountAll()
    {
        IsFirst();

        _sql.Append("COUNT(*)");
        return this;
    }

    public ISelect Function(IColumn column, string funcName, string funcAlias = "")
    {
        _sql.Append(funcName).Append('(').Append(column.ShortName).Append(')');

        if (!string.IsNullOrEmpty(funcAlias))
            _sql.Append(" AS ").AppendIdentifier(funcAlias);

        return this;
    }

    public ISelect Function(string funcName, string[] parameters, string funcAlias = "")
    {
        IsFirst();
        _sql.Append(funcName).Append("('").Append(string.Join("','", parameters)).Append("')");

        if (!string.IsNullOrEmpty(funcAlias))
            _sql.Append(" AS ").AppendIdentifier(funcAlias);

        return this;
    }

    public ISelect Expression<TPostgreSqlBuilder>(ISqlBuilder<TPostgreSqlBuilder> builder)
    {
        IsFirst();

        _sql.Append('(').Append(builder.BuildWithoutEnd()).Append(") ");
        return this;
    }

    public ISelect Expression(string sql)
    {
        IsFirst();

        _sql.Append('(').Append(sql).Append(") ");
        return this;
    }

    public ISelect Now()
    {
        IsFirst();

        _sql.Append("NOW()");
        return this;
    }

    public ISelect Now(string alias)
    {
        IsFirst();

        _sql.Append("NOW() AS ").AppendIdentifier(alias);
        return this;
    }

    public ISelect Avg(IColumn column)
    {
        IsFirst();

        _sql.Append("AVG(").Append(column.ShortName).Append(')');

        if (!string.IsNullOrEmpty(column.Alias))
            _sql.Append(" AS ").AppendIdentifier(column.Alias);

        return this;
    }

    public ISelect Count(IColumn column)
    {
        IsFirst();

        _sql.Append("COUNT(").Append(column.ShortName).Append(')');

        if (!string.IsNullOrEmpty(column.Alias))
            _sql.Append(" AS ").AppendIdentifier(column.Alias);

        return this;
    }

    public ISelect CountDistinct(IColumn column)
    {
        IsFirst();

        _sql.Append("COUNT(DISTINCT(").Append(column.ShortName).Append("))");

        if (!string.IsNullOrEmpty(column.Alias))
            _sql.Append(" AS ").AppendIdentifier(column.Alias);

        return this;
    }

    public ISelect DistinctAll()
    {
        _sql.Append("DISTINCT *");
        return this;
    }

    public ISelect Distinct(IColumn column)
    {
        IsFirst();

        _sql.Append("DISTINCT ").Append(column);
        return this;
    }

    public ISelect DistinctOn(IColumn column)
    {
        IsFirst();

        _sql.Append("DISTINCT ON (").Append(column.ShortName).Append(')');

        if (!string.IsNullOrEmpty(column.Alias))
            _sql.Append(' ').AppendIdentifier(column.Alias);

        return this;
    }

    public ISelect First(IColumn column)
    {
        IsFirst();

        _sql.Append("FIRST(").Append(column.ShortName).Append(')');

        if (!string.IsNullOrEmpty(column.Alias))
            _sql.Append(" AS ").AppendIdentifier(column.Alias);

        return this;
    }

    public ISelect Last(IColumn column)
    {
        IsFirst();

        _sql.Append("LAST(").Append(column.ShortName).Append(')');

        if (!string.IsNullOrEmpty(column.Alias))
            _sql.Append(" AS ").AppendIdentifier(column.Alias);

        return this;
    }

    public ISelect Lcase(IColumn column)
    {
        IsFirst();

        _sql.Append("LCASE(").Append(column.ShortName).Append(')');

        if (!string.IsNullOrEmpty(column.Alias))
            _sql.Append(" AS ").AppendIdentifier(column.Alias);

        return this;
    }

    public ISelect Ucase(IColumn column)
    {
        IsFirst();

        _sql.Append("UCASE(").Append(column.ShortName).Append(')');

        if (!string.IsNullOrEmpty(column.Alias))
            _sql.Append(" AS ").AppendIdentifier(column.Alias);

        return this;
    }

    public ISelect Len(IColumn column)
    {
        IsFirst();

        _sql.Append("LEN(").Append(column.ShortName).Append(')');

        if (!string.IsNullOrEmpty(column.Alias))
            _sql.Append(" AS ").AppendIdentifier(column.Alias);

        return this;
    }

    public ISelect Max(IColumn column)
    {
        IsFirst();

        _sql.Append("MAX(").Append(column.ShortName).Append(')');

        if (!string.IsNullOrEmpty(column.Alias))
            _sql.Append(" AS ").AppendIdentifier(column.Alias);

        return this;
    }

    public ISelect Min(IColumn column)
    {
        IsFirst();

        _sql.Append("MIN(").Append(column.ShortName).Append(')');

        if (!string.IsNullOrEmpty(column.Alias))
            _sql.Append(" AS ").AppendIdentifier(column.Alias);

        return this;
    }

    public ISelect Sum(IColumn column)
    {
        IsFirst();

        _sql.Append("SUM(").Append(column.ShortName).Append(')');

        if (!string.IsNullOrEmpty(column.Alias))
            _sql.Append(" AS ").AppendIdentifier(column.Alias);

        return this;
    }

    public ISelect Mid(IColumn column, int start, int length)
    {
        IsFirst();

        _sql.Append("MID(").Append(column.ShortName).Append(", ").Append(start).Append(", ").Append(length).Append(')');

        if (!string.IsNullOrEmpty(column.Alias))
            _sql.Append(" AS ").AppendIdentifier(column.Alias);

        return this;
    }

    public ISelect Round(IColumn column, int length)
    {
        IsFirst();

        _sql.Append("ROUND(").Append(column.ShortName).Append(',').Append(length).Append(')');

        if (!string.IsNullOrEmpty(column.Alias))
            _sql.Append(" AS ").AppendIdentifier(column.Alias);

        return this;
    }

    public ISelect CastTo<TFrom>(TFrom sqlType) where TFrom : Enum
    {
        var castedType = SqlHelper.ConvertEnum<PostgreSqlTypes>(sqlType);
        _sql.Append("::").Append(SqlHelper.ToString(castedType));
        return this;
    }

    public ISelect FunctionOver(IColumn column, string funcName, Action<IWindowOver> configure, string funcAlias = "")
    {
        var over = new WindowOverClause();
        configure(over);
        return FunctionOver(column, funcName, over.ToString(), funcAlias);
    }

    public ISelect FunctionOver(IColumn column, string funcName, string overClauseSql, string funcAlias = "")
    {
        IsFirst();

        _sql.Append(funcName).Append('(').Append(column.ShortName).Append(") OVER (").Append(overClauseSql).Append(')');

        if (!string.IsNullOrEmpty(funcAlias))
            _sql.Append(" AS ").AppendIdentifier(funcAlias);

        return this;
    }

    public ISelect RowNumberOver(Action<IWindowOver> configure, string funcAlias = "")
    {
        var over = new WindowOverClause();
        configure(over);
        return RowNumberOver(over.ToString(), funcAlias);
    }

    public ISelect RowNumberOver(string overClauseSql, string funcAlias = "")
    {
        IsFirst();

        _sql.Append("ROW_NUMBER() OVER (").Append(overClauseSql).Append(')');

        if (!string.IsNullOrEmpty(funcAlias))
            _sql.Append(" AS ").AppendIdentifier(funcAlias);

        return this;
    }

    public ISelect CountAllOver(Action<IWindowOver> configure, string funcAlias = "")
    {
        var over = new WindowOverClause();
        configure(over);
        return CountAllOver(over.ToString(), funcAlias);
    }

    public ISelect CountAllOver(string overClauseSql, string funcAlias = "")
    {
        IsFirst();

        _sql.Append("COUNT(*) OVER (").Append(overClauseSql).Append(')');

        if (!string.IsNullOrEmpty(funcAlias))
            _sql.Append(" AS ").AppendIdentifier(funcAlias);

        return this;
    }

    public override string ToString()
    {
        var sql = _sql.Length == 0 ? "*" : _sql.ToString();
        return sql;
    }

    private void IsFirst()
    {
        if (!_isFirst)
            _sql.Append(", ");
        _isFirst = false;
    }
}