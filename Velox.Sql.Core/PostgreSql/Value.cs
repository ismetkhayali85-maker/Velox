using System;
using System.Globalization;
using System.Text;
using Velox.Sql.Core.Impl;
using Velox.Sql.Core.Interfaces;

namespace Velox.Sql.Core.PostgreSql;

public sealed class Value : IValue
{
    private readonly StringBuilder _sql;

    public Value()
    {
        _sql = new StringBuilder("null");
    }

    public Value(int value)
    {
        _sql = new StringBuilder(value.ToString());
    }

    public Value(DateTime value)
    {
        _sql = new StringBuilder("'")
            .Append(value.ToString(CultureInfo.InvariantCulture))
            .Append("'");
    }

    public Value(string value, bool isVariable = false, bool isRaw = false)
    {
        _sql = new StringBuilder();
        if (isRaw)
        {
            _sql.Append(value);
            return;
        }

        var escaped = value?.Replace("'", "''");
        if (isVariable)
            _sql.Append('@').Append(escaped);
        else
            _sql.Append('\'').Append(escaped).Append('\'');
    }

    public Value(string columnName, string tableAlias)
    {
        _sql = new StringBuilder()
            .AppendIdentifier(tableAlias)
            .Append('.')
            .AppendIdentifier(columnName);
    }

    public Value(ISqlBuilder<PostgreSqlBuilder> nestedBuilder)
    {
        _sql = new StringBuilder(" (")
            .Append(nestedBuilder.BuildWithoutEnd())
            .Append(')');
    }

    public override string ToString()
    {
        return _sql.ToString();
    }

    public IValue CastTo<TFrom>(TFrom type) where TFrom : Enum
    {
        var castedType = SqlHelper.ConvertEnum<PostgreSqlTypes>(type);
        _sql.Append("::")
            .Append(SqlHelper.ToString(castedType));
        return this;
    }
}