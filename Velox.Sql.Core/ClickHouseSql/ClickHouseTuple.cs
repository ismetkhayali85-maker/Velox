using System;
using System.Collections.Generic;
using System.Text;
using Velox.Sql.Core.Impl;
using Velox.Sql.Core.Interfaces;

namespace Velox.Sql.Core.ClickHouseSql;

public sealed class ClickHouseTuple : IColumn
{
    private readonly bool _isShowSeparatelyTupleItems;
    private readonly Dictionary<string, string> _keyAliasDictionary;
    private readonly string _tuple;

    public ClickHouseTuple(Dictionary<string, string> keyAliasDictionary, string tupleAlias,
        bool isShowSeparatelyTupleItems)
    {
        _keyAliasDictionary = keyAliasDictionary;
        _isShowSeparatelyTupleItems = isShowSeparatelyTupleItems;
        Alias = tupleAlias;
        _tuple = string.Join(",", _keyAliasDictionary.Keys);
        IsShowSeparatelyTupleItems = isShowSeparatelyTupleItems;
    }

    public bool IsShowSeparatelyTupleItems { get; }

    public string Name
    {
        get
        {
            if (!_isShowSeparatelyTupleItems)
                return $"({_tuple})";

            var sb = new StringBuilder();
            var index = 1;
            foreach (var item in _keyAliasDictionary)
            {
                if (sb.Length > 0)
                    sb.Append(", ");

                sb.Append(Alias)
                    .Append('.')
                    .Append(index)
                    .Append(" AS ")
                    .AppendIdentifier(item.Value);
                index++;
            }

            return sb.ToString();
        }
    }

    public string ShortName => $"({_tuple})";

    public string Alias { get; }

    IColumn ISqlConvertable<IColumn>.CastTo<TFrom>(TFrom type)
    {
        throw new NotImplementedException();
    }
}