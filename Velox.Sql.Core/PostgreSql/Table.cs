using Velox.Sql.Core.Impl;
using Velox.Sql.Core.Interfaces;

namespace Velox.Sql.Core.PostgreSql;

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
        Name = name;
        Schema = schema ?? string.Empty;

        var sb = SqlStringBuilderPool.Rent();
        try
        {
            if (!string.IsNullOrEmpty(Schema))
                sb.AppendIdentifier(Schema).Append('.');

            sb.AppendIdentifier(Name);
            _short = sb.ToString();
        }
        finally
        {
            SqlStringBuilderPool.Return(sb);
        }

        Alias = null;
    }

    public Table(string schema, string name, string alias)
        : this(schema, name)
    {
        if (string.IsNullOrEmpty(alias)) return;

        var aliasSb = SqlStringBuilderPool.Rent();
        try
        {
            aliasSb.AppendIdentifier(alias);
            Alias = aliasSb.ToString();
        }
        finally
        {
            SqlStringBuilderPool.Return(aliasSb);
        }

        var initSb = SqlStringBuilderPool.Rent();
        try
        {
            if (!string.IsNullOrEmpty(Schema))
                initSb.AppendIdentifier(Schema).Append('.');

            initSb.AppendIdentifier(Name).Append(" AS ").Append(Alias);
            _init = initSb.ToString();
        }
        finally
        {
            SqlStringBuilderPool.Return(initSb);
        }
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