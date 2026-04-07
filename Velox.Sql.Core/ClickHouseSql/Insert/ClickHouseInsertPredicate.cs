using System.Collections.Generic;
using System.Runtime.InteropServices;
using System.Text;
using Velox.Sql.Core.Impl;

namespace Velox.Sql.Core.ClickHouseSql.Insert;

public sealed class ClickHouseInsertPredicate
{
    private readonly StringBuilder _sql;
    private bool _isFirst;

    public ClickHouseInsertPredicate()
    {
        _sql = new StringBuilder();
        _isFirst = true;
    }

    public ClickHouseInsertPredicate InsertValuePairs(Dictionary<string, ClickHouseValue> valuePairs)
    {
        _sql.Append(" (");
        bool firstKey = true;
        foreach (var key in valuePairs.Keys)
        {
            if (!firstKey) _sql.Append(',');
            _sql.AppendIdentifier(key);
            firstKey = false;
        }
        _sql.Append(") VALUES(");

        foreach (var value in valuePairs)
        {
            IsFirst();
            _sql.Append(value.Value);
        }

        _sql.Append(")");
        return this;
    }

    public ClickHouseInsertPredicate BulkInsertValuePairs(List<Dictionary<string, ClickHouseValue>> valuePairs)
    {
        if (valuePairs == null || valuePairs.Count == 0) return this;

        _sql.Append(" (");
        bool firstKey = true;
        foreach (var key in valuePairs[0].Keys)
        {
            if (!firstKey) _sql.Append(',');
            _sql.AppendIdentifier(key);
            firstKey = false;
        }
        _sql.Append(") VALUES ");

        int indexCount = 0;
        foreach (ref var item in CollectionsMarshal.AsSpan(valuePairs))
        {
            var isLocalFist = true;
            _sql.Append(indexCount > 0 ? ",(" : "(");
            foreach (var value in item)
            {
                if (isLocalFist)
                    isLocalFist = false;
                else
                    _sql.Append(", ");

                _sql.Append(value.Value);
            }
            _sql.Append(")");
            indexCount++;
        }

        return this;
    }

    public override string ToString()
    {
        return _sql.ToString();
    }

    private bool IsFirst()
    {
        if (_isFirst)
            _isFirst = false;
        else
            _sql.Append(", ");

        return _isFirst;
    }
}