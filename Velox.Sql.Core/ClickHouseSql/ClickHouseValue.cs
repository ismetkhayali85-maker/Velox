using System;
using System.Collections.Generic;
using System.Text;
using Velox.Sql.Core.Interfaces;

namespace Velox.Sql.Core.ClickHouseSql;

public sealed class ClickHouseValue : IValue
{
    private readonly StringBuilder _sql;
    public bool IsNull { get; private set; }

    public ClickHouseValue()
    {
        _sql = new StringBuilder();
        _sql.Append("NULL");
        IsNull = true;
    }

    public ClickHouseValue(string value, bool isNeedQuotes = true)
    {
        _sql = new StringBuilder();
        if (value == null || value == "null")
        {
            IsNull = true;
            _sql.Append("NULL");
            return;
        }

        if (isNeedQuotes)
        {
            _sql.Append('\'').Append(Escape(value)).Append('\'');
        }
        else
            _sql.Append(value);
    }

    public ClickHouseValue(List<string> values)
    {
        _sql = new StringBuilder();
        _sql.Append('[');
        _sql.Append(string.Join(", ", values));
        _sql.Append(']');
    }

    public ClickHouseValue(string type, List<string> values)
    {
        _sql = new StringBuilder();
        _sql.Append(type).Append('(');
        _sql.Append(string.Join(", ", values));
        _sql.Append(')');
    }

    private string Escape(string value)
    {
        if (string.IsNullOrEmpty(value)) return value;
        return value.Replace("\\", "\\\\").Replace("'", "\\'");
    }

    public override string ToString()
    {
        return _sql.ToString();
    }

    public ClickHouseValue Array(List<string> values)
    {
        _sql.Append('[');
        _sql.Append(string.Join(", ", values));
        _sql.Append(']');
        return this;
    }

    public ClickHouseValue ArrayItem(List<string> values)
    {
        return Array(values);
    }

    public ClickHouseValue ArrayOfArrayItem(List<List<string>> values)
    {
        _sql.Append('[');
        for (var i = 0; i < values.Count; i++)
        {
            _sql.Append('[');
            _sql.Append(string.Join(", ", values[i]));
            _sql.Append(']');
            if (i < values.Count - 1)
                _sql.Append(", ");
        }

        _sql.Append(']');
        return this;
    }

    public ClickHouseValue Add(string value)
    {
        _sql.Append(value);
        return this;
    }

    public IValue CastTo<TFrom>(TFrom type) where TFrom : Enum
    {
        _sql.Append("::").Append(type.ToString());
        return this;
    }
}