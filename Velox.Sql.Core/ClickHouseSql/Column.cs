using System;
using Velox.Sql.Core.Impl;
using Velox.Sql.Core.Interfaces;
namespace Velox.Sql.Core.ClickHouseSql;

public sealed class Column : IColumn
{
    private readonly string _columnAlias;
    private readonly string _columnName;
    private readonly string _tableName;

    public Column(string columnName, string columnAlias = "")
    {
        _columnName = columnName;
        _columnAlias = columnAlias;
    }

    public Column(string tableName, string columnName, string columnAlias)
    {
        _tableName = tableName;
        _columnName = columnName;
        _columnAlias = columnAlias;
    }

    public IColumn CastTo<TFrom>(TFrom type) where TFrom : Enum
    {
        throw new NotImplementedException();
    }

    public string Name
    {
        get
        {
            if (string.IsNullOrEmpty(_columnAlias))
                return ShortName;

            var sb = SqlStringBuilderPool.Rent();
            try
            {
                sb.Append(ShortName).Append(" AS ").AppendIdentifier(_columnAlias);
                return sb.ToString();
            }
            finally
            {
                SqlStringBuilderPool.Return(sb);
            }
        }
    }

    public string ShortName
    {
        get
        {
            var sb = SqlStringBuilderPool.Rent();
            try
            {
                if (!string.IsNullOrEmpty(_tableName))
                    sb.AppendIdentifier(_tableName).Append('.');

                sb.AppendIdentifier(_columnName);
                return sb.ToString();
            }
            finally
            {
                SqlStringBuilderPool.Return(sb);
            }
        }
    }

    public string Alias => _columnAlias;

    public override string ToString()
    {
        return Name;
    }
}