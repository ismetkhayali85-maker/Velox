using System;
using System.Collections.Generic;
using System.Runtime.InteropServices;
using Velox.Sql.Core.Interfaces;
using Velox.Sql.Impl.Map;
using ChTable = Velox.Sql.Core.ClickHouseSql.Table;
using PgTable = Velox.Sql.Core.PostgreSql.Table;

namespace Velox.Sql.Impl;

public interface ISqlConfiguration
{
    IClassMapper GetMap(Type type);
    ITable GetTable(Type type);
}

/// <summary>PostgreSQL: maps entities to <see cref="PgTable"/> only (no other PostgreSql types here).</summary>
public sealed class PgSqlConfiguration : ISqlConfiguration
{
    private readonly Dictionary<Type, IClassMapper> _mappers = new Dictionary<Type, IClassMapper>();
    private readonly Dictionary<Type, PgTable> _tables = new Dictionary<Type, PgTable>();

    public PgSqlConfiguration(List<IClassMapper> mappers)
    {
        foreach (var item in CollectionsMarshal.AsSpan(mappers))
        {
            if (item.EntityType != null)
            {
                _mappers.Add(item.EntityType, item);
                _tables.Add(item.EntityType, new PgTable(item.SchemaName, item.TableName));
            }
        }
    }

    public IClassMapper GetMap(Type type) => _mappers[type];

    public PgTable GetTable(Type type) => _tables[type];

    ITable ISqlConfiguration.GetTable(Type type) => GetTable(type);
}

public sealed class ClickHouseSqlConfiguration : ISqlConfiguration
{
    private readonly Dictionary<Type, IClassMapper> _mappers = new Dictionary<Type, IClassMapper>();
    private readonly Dictionary<Type, ChTable> _tables = new Dictionary<Type, ChTable>();

    public ClickHouseSqlConfiguration(List<IClassMapper> mappers)
    {
        foreach (var item in CollectionsMarshal.AsSpan(mappers))
        {
            _mappers.Add(item.EntityType, item);
            _tables.Add(item.EntityType, new ChTable(item.TableName));
        }
    }

    public IClassMapper GetMap(Type type) => _mappers[type];

    public ChTable GetTable(Type type) => _tables[type];

    ITable ISqlConfiguration.GetTable(Type type) => GetTable(type);
}
