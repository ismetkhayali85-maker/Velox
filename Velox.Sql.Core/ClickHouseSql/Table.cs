using System;
using System.Text;
using Velox.Sql.Core.Impl;
using Velox.Sql.Core.Interfaces;

namespace Velox.Sql.Core.ClickHouseSql;

public sealed class Table : ITable
{
    private readonly string _init;
    private readonly string _short;

    public Table(string name)
        : this(string.Empty, name)
    {
    }

    public Table(string schema, string name)
    {
        Name = name ?? throw new ArgumentNullException(nameof(name));
        Schema = schema ?? string.Empty;

        var sb = new StringBuilder();
        if (!string.IsNullOrEmpty(Schema))
        {
            sb.AppendIdentifier(Schema).Append('.');
        }

        sb.AppendIdentifier(Name);
        _short = sb.ToString();
        Alias = null;
    }

    public Table(string schema, string name, string alias)
        : this(schema, name)
    {
        if (string.IsNullOrEmpty(alias)) return;

        var aliasSb = new StringBuilder();
        aliasSb.AppendIdentifier(alias);
        Alias = aliasSb.ToString();

        var initSb = new StringBuilder();
        if (!string.IsNullOrEmpty(Schema))
        {
            initSb.AppendIdentifier(Schema).Append('.');
        }

        initSb.AppendIdentifier(Name).Append(" AS ").Append(Alias);
        _init = initSb.ToString();
    }


    public string Schema { get; }
    public string Name { get; }
    public string Alias { get; }

    public string Init()
    {
        return string.IsNullOrEmpty(Alias) ? _short : _init;
    }

    public override string ToString()
    {
        return string.IsNullOrEmpty(Alias) ? _short : Alias;
    }
}