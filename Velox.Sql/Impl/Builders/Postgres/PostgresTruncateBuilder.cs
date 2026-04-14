using System;
using System.Collections.Generic;
using Velox.Sql.Core.Impl;
using Velox.Sql.Core.PostgreSql;
using Velox.Sql.Impl.Map;
using Velox.Sql.Interfaces;

namespace Velox.Sql.Impl.Builders.Postgres;

public sealed class PostgresTruncateBuilder<TEntity> : PostgresBuilderBase<TEntity>, IPostgresTruncateBuilder<TEntity>
{
    private readonly List<string> _additionalSql = new();
    private PostgreSqlTruncateIdentityOption _identity = PostgreSqlTruncateIdentityOption.Unspecified;
    private PostgreSqlTruncateReferentialOption _referential = PostgreSqlTruncateReferentialOption.Unspecified;

    public PostgresTruncateBuilder(PgSqlConfiguration config, PostgreSqlBuilder builder)
        : base(config, builder)
    {
    }

    public IPostgresTruncateBuilder<TEntity> RestartIdentity()
    {
        if (_identity == PostgreSqlTruncateIdentityOption.ContinueIdentity)
            throw new InvalidOperationException("TRUNCATE cannot combine RESTART IDENTITY with CONTINUE IDENTITY.");
        _identity = PostgreSqlTruncateIdentityOption.RestartIdentity;
        return this;
    }

    public IPostgresTruncateBuilder<TEntity> ContinueIdentity()
    {
        if (_identity == PostgreSqlTruncateIdentityOption.RestartIdentity)
            throw new InvalidOperationException("TRUNCATE cannot combine CONTINUE IDENTITY with RESTART IDENTITY.");
        _identity = PostgreSqlTruncateIdentityOption.ContinueIdentity;
        return this;
    }

    public IPostgresTruncateBuilder<TEntity> Cascade()
    {
        if (_referential == PostgreSqlTruncateReferentialOption.Restrict)
            throw new InvalidOperationException("TRUNCATE cannot combine CASCADE with RESTRICT.");
        _referential = PostgreSqlTruncateReferentialOption.Cascade;
        return this;
    }

    public IPostgresTruncateBuilder<TEntity> Restrict()
    {
        if (_referential == PostgreSqlTruncateReferentialOption.Cascade)
            throw new InvalidOperationException("TRUNCATE cannot combine RESTRICT with CASCADE.");
        _referential = PostgreSqlTruncateReferentialOption.Restrict;
        return this;
    }

    public IPostgresTruncateBuilder<TEntity> AddSql(string sql)
    {
        _additionalSql.Add(sql);
        return this;
    }

    public override SqlQuery ToSql()
    {
        _currentParameters = new Dictionary<string, object>();
        return new SqlQuery
        {
            Sql = GetSqlInternal(true),
            Parameters = _currentParameters
        };
    }

    public override string ToDebugSql()
    {
        _currentParameters = null;
        return GetSqlInternal(true);
    }

    public override string GetSql()
    {
        return GetSqlInternal(false);
    }

    private string GetSqlInternal(bool withEnd)
    {
        _builder = new PostgreSqlBuilder();
        IClassMapper map = _config.GetMap(typeof(TEntity));
        var table = new Table(map.SchemaName, map.TableName);

        _builder.Truncate(table, _identity, _referential);
        foreach (string s in _additionalSql)
            _builder.AddSql(s);

        return withEnd ? _builder.Build() : _builder.BuildWithoutEnd();
    }
}
