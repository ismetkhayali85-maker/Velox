using System;
using Velox.Sql.Core.Impl;
using Velox.Sql.Core.Interfaces;

namespace Velox.Sql.Core.PostgreSql;

public sealed class Column : IColumn
{
    private PostgreSqlTypes _castTo;
    private readonly string _columnName;
    private readonly Table _table;

    public Column(Table table, string columnName, string columnAlias = "")
    {
        _columnName = columnName;
        Alias = columnAlias;
        _table = table;
        _castTo = PostgreSqlTypes.Empty;
    }

    public Column(string table, string columnName, string columnAlias = "")
        : this(new Table(table), columnName, columnAlias)
    {
    }

    public string Name
    {
        get
        {
            var sb = SqlStringBuilderPool.Rent();
            try
            {
                sb.AppendTable(_table);

                if (_table != null && !string.IsNullOrEmpty(_table.ToString()))
                    sb.Append('.');

                sb.AppendIdentifier(_columnName);

                if (_castTo != PostgreSqlTypes.Empty)
                    sb.Append("::").Append(SqlHelper.ToString(_castTo));

                if (!string.IsNullOrEmpty(Alias))
                    sb.Append(" AS ").AppendIdentifier(Alias);

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
                sb.AppendTable(_table);

                if (_table != null && !string.IsNullOrEmpty(_table.ToString()))
                    sb.Append('.');

                sb.AppendIdentifier(_columnName);

                if (_castTo != PostgreSqlTypes.Empty)
                    sb.Append("::").Append(SqlHelper.ToString(_castTo));

                return sb.ToString();
            }
            finally
            {
                SqlStringBuilderPool.Return(sb);
            }
        }
    }

    public string Alias { get; }

    public IColumn CastTo<TFrom>(TFrom sqlType) where TFrom : Enum
    {
        _castTo = SqlHelper.ConvertEnum<PostgreSqlTypes>(sqlType);
        return this;
    }

    public string GetShortName()
    {
        var sb = SqlStringBuilderPool.Rent();
        try
        {
            sb.AppendTable(_table);

            if (_table != null && !string.IsNullOrEmpty(_table.ToString()))
                sb.Append('.');

            sb.AppendIdentifier(_columnName);
            return sb.ToString();
        }
        finally
        {
            SqlStringBuilderPool.Return(sb);
        }
    }

    public override string ToString()
    {
        return Name;
    }
}