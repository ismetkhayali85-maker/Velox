using System;
using System.Linq.Expressions;
using System.Runtime.InteropServices;
using Velox.Sql.Core.Interfaces;
using Velox.Sql.Core.PostgreSql;
using Velox.Sql.Core.Windowing;
using Velox.Sql.Expressions;
using Velox.Sql.Impl.Map;

namespace Velox.Sql.Impl.Builders.Postgres;

/// <summary>
/// Builds PostgreSQL <c>PARTITION BY</c> / <c>ORDER BY</c> inside a window <c>OVER (...)</c> from entity expressions.
/// </summary>
public sealed class PostgresWindowOverClause
{
    private readonly WindowOverClause _inner = new();
    private readonly PgSqlConfiguration _config;

    public PostgresWindowOverClause(PgSqlConfiguration config)
    {
        _config = config;
    }

    public PostgresWindowOverClause PartitionBy<T>(Expression<Func<T, object>> expression)
    {
        IClassMapper map = _config.GetMap(typeof(T));
        var table = new Table(map.SchemaName, map.TableName);
        foreach (MemberUnaryResult exprSetting in ExpressionParser.FindMemberUnaryExpression(expression).AsSpan())
            _inner.PartitionBy(table, map.GetUserDefinedName(exprSetting.Value));
        return this;
    }

    public PostgresWindowOverClause OrderByAsc<T>(Expression<Func<T, object>> expression)
    {
        IClassMapper map = _config.GetMap(typeof(T));
        var table = new Table(map.SchemaName, map.TableName);
        foreach (MemberUnaryResult exprSetting in ExpressionParser.FindMemberUnaryExpression(expression).AsSpan())
            _inner.OrderByAsc(table, map.GetUserDefinedName(exprSetting.Value));
        return this;
    }

    public PostgresWindowOverClause OrderByDesc<T>(Expression<Func<T, object>> expression)
    {
        IClassMapper map = _config.GetMap(typeof(T));
        var table = new Table(map.SchemaName, map.TableName);
        foreach (MemberUnaryResult exprSetting in ExpressionParser.FindMemberUnaryExpression(expression).AsSpan())
            _inner.OrderByDesc(table, map.GetUserDefinedName(exprSetting.Value));
        return this;
    }

    internal string Build() => _inner.ToString();
}
