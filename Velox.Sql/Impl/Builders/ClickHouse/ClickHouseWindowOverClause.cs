using System;
using System.Linq.Expressions;
using System.Runtime.InteropServices;
using Velox.Sql.Core.ClickHouseSql;
using Velox.Sql.Core.Interfaces;
using Velox.Sql.Core.Windowing;
using Velox.Sql.Expressions;
using Velox.Sql.Impl.Map;

namespace Velox.Sql.Impl.Builders.ClickHouse;

/// <summary>
/// Builds ClickHouse <c>PARTITION BY</c> / <c>ORDER BY</c> inside <c>OVER (...)</c> from entity expressions.
/// </summary>
public sealed class ClickHouseWindowOverClause
{
    private readonly WindowOverClause _inner = new();
    private readonly ClickHouseSqlConfiguration _config;

    public ClickHouseWindowOverClause(ClickHouseSqlConfiguration config)
    {
        _config = config;
    }

    public ClickHouseWindowOverClause PartitionBy<T>(Expression<Func<T, object>> expression)
    {
        IClassMapper map = _config.GetMap(typeof(T));
        var table = new Table(map.TableName);
        foreach (MemberUnaryResult exprSetting in ExpressionParser.FindMemberUnaryExpression(expression).AsSpan())
            _inner.PartitionBy(table, map.GetUserDefinedName(exprSetting.Value));
        return this;
    }

    public ClickHouseWindowOverClause OrderByAsc<T>(Expression<Func<T, object>> expression)
    {
        IClassMapper map = _config.GetMap(typeof(T));
        var table = new Table(map.TableName);
        foreach (MemberUnaryResult exprSetting in ExpressionParser.FindMemberUnaryExpression(expression).AsSpan())
            _inner.OrderByAsc(table, map.GetUserDefinedName(exprSetting.Value));
        return this;
    }

    public ClickHouseWindowOverClause OrderByDesc<T>(Expression<Func<T, object>> expression)
    {
        IClassMapper map = _config.GetMap(typeof(T));
        var table = new Table(map.TableName);
        foreach (MemberUnaryResult exprSetting in ExpressionParser.FindMemberUnaryExpression(expression).AsSpan())
            _inner.OrderByDesc(table, map.GetUserDefinedName(exprSetting.Value));
        return this;
    }

    internal string Build() => _inner.ToString();
}
