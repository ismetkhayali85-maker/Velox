using System;
using System.Collections.Generic;
using Velox.Sql.Core.ClickHouseSql;
using Velox.Sql.Core.Impl;
using Velox.Sql.Impl.Map;
using Velox.Sql.Interfaces;

namespace Velox.Sql.Impl.Builders.ClickHouse;

public sealed class ClickHouseTruncateBuilder<TEntity> : ClickHouseBuilderBase<TEntity>, IClickHouseTruncateBuilder<TEntity>
{
    private readonly List<string> _additionalSql = new();
    private bool _ifExists;
    private string _onCluster;

    public ClickHouseTruncateBuilder(ClickHouseSqlConfiguration config, ClickHouseSqlBuilder builder)
        : base(config, builder)
    {
    }

    public IClickHouseTruncateBuilder<TEntity> IfExists()
    {
        _ifExists = true;
        return this;
    }

    public IClickHouseTruncateBuilder<TEntity> OnCluster(string clusterName)
    {
        if (string.IsNullOrWhiteSpace(clusterName))
            throw new ArgumentException("Cluster name is required.", nameof(clusterName));
        _onCluster = clusterName;
        return this;
    }

    public IClickHouseTruncateBuilder<TEntity> AddSql(string sql)
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
        _builder = new ClickHouseSqlBuilder();
        IClassMapper map = _config.GetMap(typeof(TEntity));
        var table = new Table(map.TableName);

        _builder.Truncate(table, _ifExists, _onCluster);
        foreach (string s in _additionalSql)
            _builder.AddSql(s);

        return withEnd ? _builder.Build() : _builder.BuildWithoutEnd();
    }
}
