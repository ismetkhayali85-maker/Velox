using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Runtime.InteropServices;
using Velox.Sql.Core.ClickHouseSql;
using Velox.Sql.Core.ClickHouseSql.Select;
using Velox.Sql.Core.ClickHouseSql.Where;
using Velox.Sql.Core.ClickHouseSql.GroupBy;
using Velox.Sql.Core.ClickHouseSql.OrderBy;
using Velox.Sql.Core.Impl;
using Velox.Sql.Core.Interfaces;
using Velox.Sql.Expressions;
using Velox.Sql.Impl;
using Velox.Sql.Impl.Map;
using Velox.Sql.Impl.Clauses;
using Velox.Sql.Interfaces;

namespace Velox.Sql.Impl.Builders.ClickHouse;

public class ClickHouseSelectBuilder<TEntity> : ClickHouseBuilderBase<TEntity>, IClickHouseSelectBuilder<TEntity>
{
    protected readonly List<Action<IClickHouseSelect>> _selectPredicates = new();
    protected readonly List<Func<string>> _joinPredicates = new();
    protected readonly List<Action<ClickHouseWherePredicate>> _wherePredicates = new();
    protected readonly List<Action<ClickHouseGroupByItem>> _groupByPredicates = new();
    protected readonly List<Action<ClickHouseOrder>> _orderPredicates = new();
    protected readonly List<(string op, string sql)> _setOperations = new();
    protected readonly List<(bool Recursive, string Alias, Func<ClickHouseSqlConfiguration, Dictionary<string, object>, string> Build)> _commonTableExpressions = new();
    protected Action<IHaving> _havingPredicates;
    
    protected ITable _fromTable;
    protected bool _isFinal;
    protected ulong? _limit;
    protected ulong? _offset;

    public ClickHouseSelectBuilder(ClickHouseSqlConfiguration config, ClickHouseSqlBuilder builder) 
        : base(config, builder)
    {
    }

    public IClickHouseSelectBuilder<TEntity> Select(Action<IClickHouseSelect> action)
    {
        _selectPredicates.Add(action);
        return this;
    }

    public IClickHouseSelectBuilder<TEntity> Select(Expression<Func<TEntity, object>> expr = null, string alias = null)
    {
        IClassMapper map = _config.GetMap(typeof(TEntity));
        if (expr == null)
        {
            RecursiveParseSelect<TEntity>();
            return this;
        }

        MemberUnaryResult[] exprSettings = ExpressionParser.FindMemberUnaryExpression(expr);
        foreach (MemberUnaryResult exprSetting in exprSettings.AsSpan())
            _selectPredicates.Add(x =>
                x.Column(new Column(map.TableName, map.GetUserDefinedName(exprSetting.Value), string.IsNullOrEmpty(alias) ? exprSetting.Value : alias)));

        return this;
    }

    public IClickHouseSelectBuilder<TEntity> Select<T>(Expression<Func<T, object>> expr = null, string alias = null)
    {
        IClassMapper map = _config.GetMap(typeof(T));
        if (expr == null)
        {
            RecursiveParseSelect<T>();
            return this;
        }

        MemberUnaryResult[] exprSettings = ExpressionParser.FindMemberUnaryExpression(expr);
        foreach (MemberUnaryResult exprSetting in exprSettings.AsSpan())
            _selectPredicates.Add(x =>
                x.Column(new Column(map.TableName, map.GetUserDefinedName(exprSetting.Value), string.IsNullOrEmpty(alias) ? exprSetting.Value : alias)));

        return this;
    }

    public IClickHouseSelectBuilder<TEntity> SelectAll<T>()
    {
        RecursiveParseSelect<T>();
        return this;
    }

    public IClickHouseSelectBuilder<TEntity> From<T>(string alias = null)
    {
        IClassMapper map = _config.GetMap(typeof(T));
        _fromTable = new Table(map.SchemaName, map.TableName, alias);
        return this;
    }

    public IClickHouseSelectBuilder<TEntity> Where<T>(Expression<Func<T, bool>> whereExpr)
    {
        if (whereExpr == null) return this;

        IClassMapper map = _config.GetMap(typeof(T));
        ExpressionResultEx exprSettings = ExpressionParser.FindExpressionEx(whereExpr);

        _wherePredicates.Add(w =>
        {
            var group = new ClickHouseWherePredicate();
            ExpressionItemEx item = exprSettings.Settings;
            ParseWhereExpression(group, item, map);
            w.Append($"({group.ToSql()})");
        });

        return this;
    }

    public IClickHouseSelectBuilder<TEntity> Where(Expression<Func<TEntity, bool>> whereExpr)
    {
        return Where<TEntity>(whereExpr);
    }

    private string BuildBoolExpressionSql<T>(Expression<Func<T, bool>> predicate)
    {
        IClassMapper map = _config.GetMap(typeof(T));
        ExpressionResultEx exprSettings = ExpressionParser.FindExpressionEx(predicate);
        var group = new ClickHouseWherePredicate();
        ParseWhereExpression(group, exprSettings.Settings, map);
        var sql = group.ToSql();
        if (sql.Length >= 2 && sql[0] == '(' && sql[^1] == ')')
            return sql.Substring(1, sql.Length - 2);
        return sql;
    }

    private enum ConditionalIfAggKind { Sum, Avg, Min, Max }

    private static void ApplyIfAggOnColumn(IClickHouseSelect s, ConditionalIfAggKind kind, IColumn col, string condSql,
        string outAlias)
    {
        switch (kind)
        {
            case ConditionalIfAggKind.Sum:
                s.SumIf(col, condSql, outAlias);
                break;
            case ConditionalIfAggKind.Avg:
                s.AvgIf(col, condSql, outAlias);
                break;
            case ConditionalIfAggKind.Min:
                s.MinIf(col, condSql, outAlias);
                break;
            case ConditionalIfAggKind.Max:
                s.MaxIf(col, condSql, outAlias);
                break;
            default:
                throw new ArgumentOutOfRangeException(nameof(kind));
        }
    }

    private static void ApplyIfAggOnValueSql(IClickHouseSelect s, ConditionalIfAggKind kind, string valueSql, string condSql,
        string alias)
    {
        switch (kind)
        {
            case ConditionalIfAggKind.Sum:
                s.SumIf(valueSql, condSql, alias);
                break;
            case ConditionalIfAggKind.Avg:
                s.AvgIf(valueSql, condSql, alias);
                break;
            case ConditionalIfAggKind.Min:
                s.MinIf(valueSql, condSql, alias);
                break;
            case ConditionalIfAggKind.Max:
                s.MaxIf(valueSql, condSql, alias);
                break;
            default:
                throw new ArgumentOutOfRangeException(nameof(kind));
        }
    }

    private IClickHouseSelectBuilder<TEntity> AddIfFromColumn(
        ConditionalIfAggKind kind,
        Expression<Func<TEntity, object>> columnExpr,
        string conditionSql,
        string alias)
    {
        IClassMapper map = _config.GetMap(typeof(TEntity));
        MemberUnaryResult[] cols = ExpressionParser.FindMemberUnaryExpression(columnExpr);
        foreach (MemberUnaryResult exprSetting in cols.AsSpan())
        {
            var col = new Column(map.TableName, map.GetUserDefinedName(exprSetting.Value), "");
            string outAlias = string.IsNullOrEmpty(alias) ? exprSetting.Value : alias;
            ConditionalIfAggKind k = kind;
            _selectPredicates.Add(s => ApplyIfAggOnColumn(s, k, col, conditionSql, outAlias));
        }

        return this;
    }

    private IClickHouseSelectBuilder<TEntity> AddIfFromValueSql(ConditionalIfAggKind kind, string valueExpressionSql,
        string conditionSql, string alias)
    {
        if (valueExpressionSql == null) throw new ArgumentNullException(nameof(valueExpressionSql));
        ConditionalIfAggKind k = kind;
        _selectPredicates.Add(s => ApplyIfAggOnValueSql(s, k, valueExpressionSql, conditionSql, alias));
        return this;
    }

    public IClickHouseSelectBuilder<TEntity> Where(Action<IWhere<TEntity>> action)
    {
        _wherePredicates.Add(w => 
        {
            action(new WhereClause<TEntity>(w, _config, ConvertTo));
        });
        return this;
    }

    public IClickHouseSelectBuilder<TEntity> InnerJoin<TNewTable, TOldTable>(
        Expression<Func<TNewTable, object>> firstSelector, Expression<Func<TOldTable, object>> secondSelector,
        Expression<Func<TNewTable, TOldTable, object>> resultSelector = null)
    {
        _joinPredicates.Add(() =>
        {
            IClassMapper fromMap = _config.GetMap(typeof(TNewTable));
            IClassMapper toMap = _config.GetMap(typeof(TOldTable));
            MemberUnaryResult[] fromSettings = ExpressionParser.FindMemberUnaryExpression(firstSelector);
            MemberUnaryResult[] toSettings = ExpressionParser.FindMemberUnaryExpression(secondSelector);

            MemberUnaryResult fromField = Array.FindLast(fromSettings, x => x.Type == typeof(TNewTable));
            MemberUnaryResult toField = Array.FindLast(toSettings, x => x.Type == typeof(TOldTable));

            var tableFrom = _config.GetTable(typeof(TNewTable));
            var tableTo = _config.GetTable(typeof(TOldTable));

            return $" INNER JOIN {tableFrom.Init()} ON {tableFrom}.\"{EscapeIdentifier(fromMap.GetUserDefinedName(fromField.Value))}\" = {tableTo}.\"{EscapeIdentifier(toMap.GetUserDefinedName(toField.Value))}\"";
        });

        if (resultSelector == null) return this;

        MemberUnaryResult[] exprSettings = ExpressionParser.FindMemberUnaryExpression(resultSelector);
        foreach (MemberUnaryResult exprSetting in exprSettings.AsSpan())
        {
            IClassMapper selectMap = _config.GetMap(exprSetting.Type);
            _selectPredicates.Add(x =>
                x.Column(new Column(selectMap.TableName, selectMap.GetUserDefinedName(exprSetting.Value), exprSetting.Value)));
        }

        return this;
    }

    public IClickHouseSelectBuilder<TEntity> LeftJoin<TNewTable, TOldTable>(
        Expression<Func<TNewTable, object>> firstSelector, Expression<Func<TOldTable, object>> secondSelector,
        Expression<Func<TNewTable, TOldTable, object>> resultSelector = null)
    {
        _joinPredicates.Add(() =>
        {
            IClassMapper fromMap = _config.GetMap(typeof(TNewTable));
            IClassMapper toMap = _config.GetMap(typeof(TOldTable));
            MemberUnaryResult[] fromSettings = ExpressionParser.FindMemberUnaryExpression(firstSelector);
            MemberUnaryResult[] toSettings = ExpressionParser.FindMemberUnaryExpression(secondSelector);

            MemberUnaryResult fromField = Array.FindLast(fromSettings, x => x.Type == typeof(TNewTable));
            MemberUnaryResult toField = Array.FindLast(toSettings, x => x.Type == typeof(TOldTable));

            var tableFrom = _config.GetTable(typeof(TNewTable));
            var tableTo = _config.GetTable(typeof(TOldTable));

            return $" LEFT JOIN {tableFrom.Init()} ON {tableFrom}.\"{EscapeIdentifier(fromMap.GetUserDefinedName(fromField.Value))}\" = {tableTo}.\"{EscapeIdentifier(toMap.GetUserDefinedName(toField.Value))}\"";
        });

        if (resultSelector == null) return this;

        MemberUnaryResult[] exprSettings = ExpressionParser.FindMemberUnaryExpression(resultSelector);
        foreach (MemberUnaryResult exprSetting in exprSettings.AsSpan())
        {
            IClassMapper selectMap = _config.GetMap(exprSetting.Type);
            _selectPredicates.Add(x =>
                x.Column(new Column(selectMap.TableName, selectMap.GetUserDefinedName(exprSetting.Value), exprSetting.Value)));
        }

        return this;
    }

    public IClickHouseSelectBuilder<TEntity> RightJoin<TNewTable, TOldTable>(
        Expression<Func<TNewTable, object>> firstSelector, Expression<Func<TOldTable, object>> secondSelector,
        Expression<Func<TNewTable, TOldTable, object>> resultSelector = null)
    {
        _joinPredicates.Add(() =>
        {
            IClassMapper fromMap = _config.GetMap(typeof(TNewTable));
            IClassMapper toMap = _config.GetMap(typeof(TOldTable));
            MemberUnaryResult[] fromSettings = ExpressionParser.FindMemberUnaryExpression(firstSelector);
            MemberUnaryResult[] toSettings = ExpressionParser.FindMemberUnaryExpression(secondSelector);

            MemberUnaryResult fromField = Array.FindLast(fromSettings, x => x.Type == typeof(TNewTable));
            MemberUnaryResult toField = Array.FindLast(toSettings, x => x.Type == typeof(TOldTable));

            var tableFrom = _config.GetTable(typeof(TNewTable));
            var tableTo = _config.GetTable(typeof(TOldTable));

            return $" RIGHT JOIN {tableFrom.Init()} ON {tableFrom}.\"{EscapeIdentifier(fromMap.GetUserDefinedName(fromField.Value))}\" = {tableTo}.\"{EscapeIdentifier(toMap.GetUserDefinedName(toField.Value))}\"";
        });

        if (resultSelector == null) return this;

        MemberUnaryResult[] exprSettings = ExpressionParser.FindMemberUnaryExpression(resultSelector);
        foreach (MemberUnaryResult exprSetting in exprSettings.AsSpan())
        {
            IClassMapper selectMap = _config.GetMap(exprSetting.Type);
            _selectPredicates.Add(x =>
                x.Column(new Column(selectMap.TableName, selectMap.GetUserDefinedName(exprSetting.Value), exprSetting.Value)));
        }

        return this;
    }

    public IClickHouseSelectBuilder<TEntity> FullJoin<TNewTable, TOldTable>(
        Expression<Func<TNewTable, object>> firstSelector, Expression<Func<TOldTable, object>> secondSelector,
        Expression<Func<TNewTable, TOldTable, object>> resultSelector = null)
    {
        _joinPredicates.Add(() =>
        {
            IClassMapper fromMap = _config.GetMap(typeof(TNewTable));
            IClassMapper toMap = _config.GetMap(typeof(TOldTable));
            MemberUnaryResult[] fromSettings = ExpressionParser.FindMemberUnaryExpression(firstSelector);
            MemberUnaryResult[] toSettings = ExpressionParser.FindMemberUnaryExpression(secondSelector);

            MemberUnaryResult fromField = Array.FindLast(fromSettings, x => x.Type == typeof(TNewTable));
            MemberUnaryResult toField = Array.FindLast(toSettings, x => x.Type == typeof(TOldTable));

            var tableFrom = _config.GetTable(typeof(TNewTable));
            var tableTo = _config.GetTable(typeof(TOldTable));

            return $" FULL JOIN {tableFrom.Init()} ON {tableFrom}.\"{EscapeIdentifier(fromMap.GetUserDefinedName(fromField.Value))}\" = {tableTo}.\"{EscapeIdentifier(toMap.GetUserDefinedName(toField.Value))}\"";
        });

        if (resultSelector == null) return this;

        MemberUnaryResult[] exprSettings = ExpressionParser.FindMemberUnaryExpression(resultSelector);
        foreach (MemberUnaryResult exprSetting in exprSettings.AsSpan())
        {
            IClassMapper selectMap = _config.GetMap(exprSetting.Type);
            _selectPredicates.Add(x =>
                x.Column(new Column(selectMap.TableName, selectMap.GetUserDefinedName(exprSetting.Value), exprSetting.Value)));
        }

        return this;
    }

    public IClickHouseSelectBuilder<TEntity> CrossJoin<TTable>()
    {
        _joinPredicates.Add(() => string.Concat(" CROSS JOIN ", _config.GetTable(typeof(TTable))));
        return this;
    }

    public IClickHouseSelectBuilder<TEntity> GroupBy(Expression<Func<TEntity, object>> expression = null)
    {
        var table = _config.GetTable(typeof(TEntity));

        if (expression == null)
            _groupByPredicates.Add(x => x.Item(table, "*"));
        else
        {
            var map = _config.GetMap(typeof(TEntity));
            var name = ExpressionParser.FindMemberUnaryExpression(expression).First().Value;
            _groupByPredicates.Add(x => x.Item(table, map.GetUserDefinedName(name)));
        }

        return this;
    }

    public IClickHouseSelectBuilder<TEntity> Having(Action<IHaving<TEntity>> action)
    {
        if (_havingPredicates == null)
            _havingPredicates = h => action(new HavingClause<TEntity>(h, _config, ConvertTo));
        else
            _havingPredicates += h => action(new HavingClause<TEntity>(h, _config, ConvertTo));
        return this;
    }

    public IClickHouseSelectBuilder<TEntity> OrderBy<T>(bool isAsc = true, Expression<Func<T, object>> expr = null, bool sortByAlias = false)
    {
        IClassMapper map = _config.GetMap(typeof(T));

        if (expr != null)
        {
            MemberUnaryResult[] exprSettings = ExpressionParser.FindMemberUnaryExpression(expr);
            foreach (MemberUnaryResult exprSetting in exprSettings.AsSpan())
                if (isAsc)
                    _orderPredicates.Add(order =>
                        order.Asc(sortByAlias
                            ? new Column(exprSetting.Value)
                            : new Column(map.TableName, map.GetUserDefinedName(exprSetting.Value), "")));
                else
                    _orderPredicates.Add(order =>
                        order.Desc(sortByAlias
                            ? new Column(exprSetting.Value)
                            : new Column(map.TableName, map.GetUserDefinedName(exprSetting.Value), "")));
            return this;
        }

        if (isAsc)
            _orderPredicates.Add(order =>
                order.Asc(new Column(map.TableName, map.GetUserDefinedName(map.HasKey().ColumnName), "")));
        else
            _orderPredicates.Add(order =>
                order.Desc(new Column(map.TableName, map.GetUserDefinedName(map.HasKey().ColumnName), "")));

        return this;
    }

    public IClickHouseSelectBuilder<TEntity> OrderBy(bool isAsc = true, Expression<Func<TEntity, object>> expr = null, bool sortByAlias = false)
    {
        return OrderBy<TEntity>(isAsc, expr, sortByAlias);
    }

    public IClickHouseSelectBuilder<TEntity> OrderByAsc<T>(Expression<Func<T, object>> expr = null, bool sortByAlias = false) =>
        OrderBy<T>(true, expr, sortByAlias);

    public IClickHouseSelectBuilder<TEntity> OrderByDesc<T>(Expression<Func<T, object>> expr = null, bool sortByAlias = false) =>
        OrderBy<T>(false, expr, sortByAlias);

    public IClickHouseSelectBuilder<TEntity> OrderByAsc(Expression<Func<TEntity, object>> expr = null, bool sortByAlias = false) =>
        OrderBy<TEntity>(true, expr, sortByAlias);

    public IClickHouseSelectBuilder<TEntity> OrderByDesc(Expression<Func<TEntity, object>> expr = null, bool sortByAlias = false) =>
        OrderBy<TEntity>(false, expr, sortByAlias);

    public IClickHouseSelectBuilder<TEntity> Limit(uint value)
    {
        _limit = value;
        return this;
    }

    public IClickHouseSelectBuilder<TEntity> Offset(uint value)
    {
        _offset = value;
        return this;
    }

    public IClickHouseSelectBuilder<TEntity> Count(Expression<Func<TEntity, object>> expression = null, bool isDistinct = false, string alias = null)
    {
        IClassMapper map = _config.GetMap(typeof(TEntity));

        if (expression == null)
        {
            _selectPredicates.Add(x => x.CountAll(alias));
        }
        else
        {
            MemberUnaryResult[] exprSettings = ExpressionParser.FindMemberUnaryExpression(expression);

            if (isDistinct)
                foreach (MemberUnaryResult exprSetting in exprSettings.AsSpan())
                    _selectPredicates.Add(x =>
                        x.DistinctCount(new Column(map.TableName, map.GetUserDefinedName(exprSetting.Value),
                            string.IsNullOrEmpty(alias) ? exprSetting.Value : alias)));
            else
                foreach (MemberUnaryResult exprSetting in exprSettings.AsSpan())
                    _selectPredicates.Add(x =>
                        x.Count(new Column(map.TableName, map.GetUserDefinedName(exprSetting.Value),
                            string.IsNullOrEmpty(alias) ? exprSetting.Value : alias)));
        }

        return this;
    }

    public IClickHouseSelectBuilder<TEntity> Count(string alias) => Count(null, false, alias);
    public IClickHouseSelectBuilder<TEntity> Count(Expression<Func<TEntity, object>> expr, string alias) => Count(expr, false, alias);
    public IClickHouseSelectBuilder<TEntity> CountDistinct(Expression<Func<TEntity, object>> expression, string alias = null) => Count(expression, true, alias);

    public IClickHouseSelectBuilder<TEntity> Distinct(Expression<Func<TEntity, object>> expression)
    {
        IClassMapper map = _config.GetMap(typeof(TEntity));

        MemberUnaryResult[] exprSettings = ExpressionParser.FindMemberUnaryExpression(expression);
        foreach (MemberUnaryResult exprSetting in exprSettings.AsSpan())
            _selectPredicates.Add(x =>
                x.Distinct(new Column(map.TableName, map.GetUserDefinedName(exprSetting.Value), exprSetting.Value)));

        return this;
    }

    public IClickHouseSelectBuilder<TEntity> Sum(Expression<Func<TEntity, object>> expr, string alias = null)
    {
        IClassMapper map = _config.GetMap(typeof(TEntity));
        MemberUnaryResult[] exprSettings = ExpressionParser.FindMemberUnaryExpression(expr);
        foreach (MemberUnaryResult exprSetting in exprSettings.AsSpan())
            _selectPredicates.Add(x => x.Sum(new Column(map.TableName, map.GetUserDefinedName(exprSetting.Value), alias)));
        return this;
    }

    public IClickHouseSelectBuilder<TEntity> SumCount(Expression<Func<TEntity, object>> expr, string alias = null)
    {
        IClassMapper map = _config.GetMap(typeof(TEntity));
        MemberUnaryResult[] exprSettings = ExpressionParser.FindMemberUnaryExpression(expr);
        foreach (MemberUnaryResult exprSetting in exprSettings.AsSpan())
            _selectPredicates.Add(x =>
                x.SumCount(new Column(map.TableName, map.GetUserDefinedName(exprSetting.Value), alias)));
        return this;
    }

    public IClickHouseSelectBuilder<TEntity> CountIf<T>(Expression<Func<T, bool>> condition, string alias = null)
    {
        string condSql = BuildBoolExpressionSql(condition);
        _selectPredicates.Add(s => s.CountIf(condSql, alias));
        return this;
    }

    public IClickHouseSelectBuilder<TEntity> CountIf(Expression<Func<TEntity, bool>> condition, string alias = null) =>
        CountIf<TEntity>(condition, alias);

    public IClickHouseSelectBuilder<TEntity> CountIf(string conditionSql, string alias = null)
    {
        if (conditionSql == null) throw new ArgumentNullException(nameof(conditionSql));
        _selectPredicates.Add(s => s.CountIf(conditionSql, alias));
        return this;
    }

    public IClickHouseSelectBuilder<TEntity> SumIf(
        Expression<Func<TEntity, object>> columnExpr,
        Expression<Func<TEntity, bool>> condition,
        string alias = null) =>
        AddIfFromColumn(ConditionalIfAggKind.Sum, columnExpr, BuildBoolExpressionSql(condition), alias);

    public IClickHouseSelectBuilder<TEntity> SumIf(
        Expression<Func<TEntity, object>> columnExpr,
        string conditionSql,
        string alias = null)
    {
        if (conditionSql == null) throw new ArgumentNullException(nameof(conditionSql));
        return AddIfFromColumn(ConditionalIfAggKind.Sum, columnExpr, conditionSql, alias);
    }

    public IClickHouseSelectBuilder<TEntity> SumIf(
        string valueExpressionSql,
        Expression<Func<TEntity, bool>> condition,
        string alias = null) =>
        AddIfFromValueSql(ConditionalIfAggKind.Sum, valueExpressionSql, BuildBoolExpressionSql(condition), alias);

    public IClickHouseSelectBuilder<TEntity> SumIf(string valueExpressionSql, string conditionSql, string alias = null)
    {
        if (conditionSql == null) throw new ArgumentNullException(nameof(conditionSql));
        return AddIfFromValueSql(ConditionalIfAggKind.Sum, valueExpressionSql, conditionSql, alias);
    }

    public IClickHouseSelectBuilder<TEntity> AvgIf(
        Expression<Func<TEntity, object>> columnExpr,
        Expression<Func<TEntity, bool>> condition,
        string alias = null) =>
        AddIfFromColumn(ConditionalIfAggKind.Avg, columnExpr, BuildBoolExpressionSql(condition), alias);

    public IClickHouseSelectBuilder<TEntity> AvgIf(
        Expression<Func<TEntity, object>> columnExpr,
        string conditionSql,
        string alias = null)
    {
        if (conditionSql == null) throw new ArgumentNullException(nameof(conditionSql));
        return AddIfFromColumn(ConditionalIfAggKind.Avg, columnExpr, conditionSql, alias);
    }

    public IClickHouseSelectBuilder<TEntity> AvgIf(
        string valueExpressionSql,
        Expression<Func<TEntity, bool>> condition,
        string alias = null) =>
        AddIfFromValueSql(ConditionalIfAggKind.Avg, valueExpressionSql, BuildBoolExpressionSql(condition), alias);

    public IClickHouseSelectBuilder<TEntity> AvgIf(string valueExpressionSql, string conditionSql, string alias = null)
    {
        if (conditionSql == null) throw new ArgumentNullException(nameof(conditionSql));
        return AddIfFromValueSql(ConditionalIfAggKind.Avg, valueExpressionSql, conditionSql, alias);
    }

    public IClickHouseSelectBuilder<TEntity> MinIf(
        Expression<Func<TEntity, object>> columnExpr,
        Expression<Func<TEntity, bool>> condition,
        string alias = null) =>
        AddIfFromColumn(ConditionalIfAggKind.Min, columnExpr, BuildBoolExpressionSql(condition), alias);

    public IClickHouseSelectBuilder<TEntity> MinIf(
        Expression<Func<TEntity, object>> columnExpr,
        string conditionSql,
        string alias = null)
    {
        if (conditionSql == null) throw new ArgumentNullException(nameof(conditionSql));
        return AddIfFromColumn(ConditionalIfAggKind.Min, columnExpr, conditionSql, alias);
    }

    public IClickHouseSelectBuilder<TEntity> MinIf(
        string valueExpressionSql,
        Expression<Func<TEntity, bool>> condition,
        string alias = null) =>
        AddIfFromValueSql(ConditionalIfAggKind.Min, valueExpressionSql, BuildBoolExpressionSql(condition), alias);

    public IClickHouseSelectBuilder<TEntity> MinIf(string valueExpressionSql, string conditionSql, string alias = null)
    {
        if (conditionSql == null) throw new ArgumentNullException(nameof(conditionSql));
        return AddIfFromValueSql(ConditionalIfAggKind.Min, valueExpressionSql, conditionSql, alias);
    }

    public IClickHouseSelectBuilder<TEntity> MaxIf(
        Expression<Func<TEntity, object>> columnExpr,
        Expression<Func<TEntity, bool>> condition,
        string alias = null) =>
        AddIfFromColumn(ConditionalIfAggKind.Max, columnExpr, BuildBoolExpressionSql(condition), alias);

    public IClickHouseSelectBuilder<TEntity> MaxIf(
        Expression<Func<TEntity, object>> columnExpr,
        string conditionSql,
        string alias = null)
    {
        if (conditionSql == null) throw new ArgumentNullException(nameof(conditionSql));
        return AddIfFromColumn(ConditionalIfAggKind.Max, columnExpr, conditionSql, alias);
    }

    public IClickHouseSelectBuilder<TEntity> MaxIf(
        string valueExpressionSql,
        Expression<Func<TEntity, bool>> condition,
        string alias = null) =>
        AddIfFromValueSql(ConditionalIfAggKind.Max, valueExpressionSql, BuildBoolExpressionSql(condition), alias);

    public IClickHouseSelectBuilder<TEntity> MaxIf(string valueExpressionSql, string conditionSql, string alias = null)
    {
        if (conditionSql == null) throw new ArgumentNullException(nameof(conditionSql));
        return AddIfFromValueSql(ConditionalIfAggKind.Max, valueExpressionSql, conditionSql, alias);
    }

    public IClickHouseSelectBuilder<TEntity> Avg(Expression<Func<TEntity, object>> expr, string alias = null)
    {
        IClassMapper map = _config.GetMap(typeof(TEntity));
        MemberUnaryResult[] exprSettings = ExpressionParser.FindMemberUnaryExpression(expr);
        foreach (MemberUnaryResult exprSetting in exprSettings.AsSpan())
            _selectPredicates.Add(x => x.Avg(new Column(map.TableName, map.GetUserDefinedName(exprSetting.Value), alias)));
        return this;
    }

    public IClickHouseSelectBuilder<TEntity> Min(Expression<Func<TEntity, object>> expr, string alias = null)
    {
        IClassMapper map = _config.GetMap(typeof(TEntity));
        MemberUnaryResult[] exprSettings = ExpressionParser.FindMemberUnaryExpression(expr);
        foreach (MemberUnaryResult exprSetting in exprSettings.AsSpan())
            _selectPredicates.Add(x => x.Min(new Column(map.TableName, map.GetUserDefinedName(exprSetting.Value), alias)));
        return this;
    }

    public IClickHouseSelectBuilder<TEntity> Max(Expression<Func<TEntity, object>> expr, string alias = null)
    {
        IClassMapper map = _config.GetMap(typeof(TEntity));
        MemberUnaryResult[] exprSettings = ExpressionParser.FindMemberUnaryExpression(expr);
        foreach (MemberUnaryResult exprSetting in exprSettings.AsSpan())
            _selectPredicates.Add(x => x.Max(new Column(map.TableName, map.GetUserDefinedName(exprSetting.Value), alias)));
        return this;
    }

    public IClickHouseSelectBuilder<TEntity> SumOver<TSum>(Expression<Func<TSum, object>> sumExpr,
        Action<ClickHouseWindowOverClause> window, string alias = null) =>
        FunctionOver(sumExpr, "sum", window, alias);

    public IClickHouseSelectBuilder<TEntity> AvgOver<TAvg>(Expression<Func<TAvg, object>> expr,
        Action<ClickHouseWindowOverClause> window, string alias = null) =>
        FunctionOver(expr, "avg", window, alias);

    public IClickHouseSelectBuilder<TEntity> MinOver<TMin>(Expression<Func<TMin, object>> expr,
        Action<ClickHouseWindowOverClause> window, string alias = null) =>
        FunctionOver(expr, "min", window, alias);

    public IClickHouseSelectBuilder<TEntity> MaxOver<TMax>(Expression<Func<TMax, object>> expr,
        Action<ClickHouseWindowOverClause> window, string alias = null) =>
        FunctionOver(expr, "max", window, alias);

    public IClickHouseSelectBuilder<TEntity> RowNumberOver(Action<ClickHouseWindowOverClause> window, string alias = "RowNumber")
    {
        _selectPredicates.Add(s =>
        {
            var w = new ClickHouseWindowOverClause(_config);
            window(w);
            s.RowNumberOver(w.Build(), alias);
        });
        return this;
    }

    public IClickHouseSelectBuilder<TEntity> CountOver(Action<ClickHouseWindowOverClause> window, string alias = null)
    {
        _selectPredicates.Add(s =>
        {
            var w = new ClickHouseWindowOverClause(_config);
            window(w);
            s.CountAllOver(w.Build(), alias);
        });
        return this;
    }

    private IClickHouseSelectBuilder<TEntity> FunctionOver<TCol>(Expression<Func<TCol, object>> columnExpr, string funcName,
        Action<ClickHouseWindowOverClause> window, string alias)
    {
        IClassMapper map = _config.GetMap(typeof(TCol));
        MemberUnaryResult[] exprSettings = ExpressionParser.FindMemberUnaryExpression(columnExpr);
        foreach (MemberUnaryResult exprSetting in exprSettings.AsSpan())
        {
            var col = new Column(map.TableName, map.GetUserDefinedName(exprSetting.Value), "");
            _selectPredicates.Add(s =>
            {
                var w = new ClickHouseWindowOverClause(_config);
                window(w);
                s.FunctionOver(col, funcName, w.Build(), string.IsNullOrEmpty(alias) ? exprSetting.Value : alias);
            });
        }

        return this;
    }

    public IClickHouseSelectBuilder<TEntity> Any(Expression<Func<TEntity, object>> action, string alias = null)
    {
        IClassMapper map = _config.GetMap(typeof(TEntity));
        MemberUnaryResult[] exprSettings = ExpressionParser.FindMemberUnaryExpression(action);
        foreach (MemberUnaryResult exprSetting in exprSettings.AsSpan())
            _selectPredicates.Add(x => x.Any(new Column(map.TableName, map.GetUserDefinedName(exprSetting.Value), string.IsNullOrEmpty(alias) ? exprSetting.Value : alias)));
        return this;
    }

    public IClickHouseSelectBuilder<TEntity> AnyRespectNulls(Expression<Func<TEntity, object>> action, string alias = null)
    {
        IClassMapper map = _config.GetMap(typeof(TEntity));
        MemberUnaryResult[] exprSettings = ExpressionParser.FindMemberUnaryExpression(action);
        foreach (MemberUnaryResult exprSetting in exprSettings.AsSpan())
            _selectPredicates.Add(x => x.AnyRespectNulls(new Column(map.TableName, map.GetUserDefinedName(exprSetting.Value), string.IsNullOrEmpty(alias) ? exprSetting.Value : alias)));
        return this;
    }

    public IClickHouseSelectBuilder<TEntity> AnyLast(Expression<Func<TEntity, object>> action, string alias = null)
    {
        IClassMapper map = _config.GetMap(typeof(TEntity));
        MemberUnaryResult[] exprSettings = ExpressionParser.FindMemberUnaryExpression(action);
        foreach (MemberUnaryResult exprSetting in exprSettings.AsSpan())
            _selectPredicates.Add(x => x.AnyLast(new Column(map.TableName, map.GetUserDefinedName(exprSetting.Value), string.IsNullOrEmpty(alias) ? exprSetting.Value : alias)));
        return this;
    }

    public IClickHouseSelectBuilder<TEntity> AnyLastRespectNulls(Expression<Func<TEntity, object>> action, string alias = null)
    {
        IClassMapper map = _config.GetMap(typeof(TEntity));
        MemberUnaryResult[] exprSettings = ExpressionParser.FindMemberUnaryExpression(action);
        foreach (MemberUnaryResult exprSetting in exprSettings.AsSpan())
            _selectPredicates.Add(x => x.AnyLastRespectNulls(new Column(map.TableName, map.GetUserDefinedName(exprSetting.Value), string.IsNullOrEmpty(alias) ? exprSetting.Value : alias)));
        return this;
    }

    public IClickHouseSelectBuilder<TEntity> AnyHeavy(Expression<Func<TEntity, object>> action, string alias = null)
    {
        IClassMapper map = _config.GetMap(typeof(TEntity));
        MemberUnaryResult[] exprSettings = ExpressionParser.FindMemberUnaryExpression(action);
        foreach (MemberUnaryResult exprSetting in exprSettings.AsSpan())
            _selectPredicates.Add(x => x.AnyHeavy(new Column(map.TableName, map.GetUserDefinedName(exprSetting.Value), string.IsNullOrEmpty(alias) ? exprSetting.Value : alias)));
        return this;
    }

    public IClickHouseSelectBuilder<TEntity> FirstValue(Expression<Func<TEntity, object>> action, string alias = null)
    {
        IClassMapper map = _config.GetMap(typeof(TEntity));
        MemberUnaryResult[] exprSettings = ExpressionParser.FindMemberUnaryExpression(action);
        foreach (MemberUnaryResult exprSetting in exprSettings.AsSpan())
            _selectPredicates.Add(x => x.FirstValue(new Column(map.TableName, map.GetUserDefinedName(exprSetting.Value), string.IsNullOrEmpty(alias) ? exprSetting.Value : alias)));
        return this;
    }

    public IClickHouseSelectBuilder<TEntity> ArgMax(
        Expression<Func<TEntity, object>> valueExpr,
        Expression<Func<TEntity, object>> byExpr,
        string alias = null) =>
        ArgAggregate(valueExpr, byExpr, "argMax", alias);

    public IClickHouseSelectBuilder<TEntity> ArgMin(
        Expression<Func<TEntity, object>> valueExpr,
        Expression<Func<TEntity, object>> byExpr,
        string alias = null) =>
        ArgAggregate(valueExpr, byExpr, "argMin", alias);

    public IClickHouseSelectBuilder<TEntity> ArgAndMax(
        Expression<Func<TEntity, object>> valueExpr,
        Expression<Func<TEntity, object>> byExpr,
        string alias = null) =>
        ArgAggregate(valueExpr, byExpr, "argAndMax", alias);

    public IClickHouseSelectBuilder<TEntity> ArgAndMin(
        Expression<Func<TEntity, object>> valueExpr,
        Expression<Func<TEntity, object>> byExpr,
        string alias = null) =>
        ArgAggregate(valueExpr, byExpr, "argAndMin", alias);

    private IClickHouseSelectBuilder<TEntity> ArgAggregate(
        Expression<Func<TEntity, object>> valueExpr,
        Expression<Func<TEntity, object>> byExpr,
        string kind,
        string alias)
    {
        IClassMapper map = _config.GetMap(typeof(TEntity));
        MemberUnaryResult[] v = ExpressionParser.FindMemberUnaryExpression(valueExpr);
        MemberUnaryResult[] b = ExpressionParser.FindMemberUnaryExpression(byExpr);
        if (v.Length != 1 || b.Length != 1)
            throw new ArgumentException($"{kind} requires exactly one property in each expression.");

        var valueCol = new Column(map.TableName, map.GetUserDefinedName(v[0].Value), "");
        var byCol = new Column(map.TableName, map.GetUserDefinedName(b[0].Value), "");
        var outAlias = string.IsNullOrEmpty(alias) ? v[0].Value : alias;

        _selectPredicates.Add(s =>
        {
            switch (kind)
            {
                case "argMax":
                    s.ArgMax(valueCol, byCol, outAlias);
                    break;
                case "argMin":
                    s.ArgMin(valueCol, byCol, outAlias);
                    break;
                case "argAndMax":
                    s.ArgAndMax(valueCol, byCol, outAlias);
                    break;
                case "argAndMin":
                    s.ArgAndMin(valueCol, byCol, outAlias);
                    break;
                default:
                    throw new ArgumentOutOfRangeException(nameof(kind), kind, null);
            }
        });
        return this;
    }

    public IClickHouseSelectBuilder<TEntity> Final()
    {
        _isFinal = true;
        return this;
    }

    public IClickHouseSelectBuilder<TEntity> AddValue(string value, string alias = null, bool isNeedQuotes = false)
    {
        _selectPredicates.Add(x => x.Value(new ClickHouseValue(value, isNeedQuotes), alias));
        return this;
    }

    public IClickHouseSelectBuilder<TEntity> AddWhereValue(string value, bool? isAnd = null)
    {
        _wherePredicates.Add(x => 
        {
            if (isAnd.HasValue)
            {
                if (isAnd.Value) x.And();
                else x.Or();
            }
            x.Append(value);
        });
        return this;
    }

    public IClickHouseSelectBuilder<TEntity> Union(IWhereSubQuery query)
    {
        _setOperations.Add((" UNION ", query.GetSql()));
        return this;
    }

    public IClickHouseSelectBuilder<TEntity> UnionAll(IWhereSubQuery query)
    {
        _setOperations.Add((" UNION ALL ", query.GetSql()));
        return this;
    }

    public IClickHouseSelectBuilder<TEntity> Intersect(IWhereSubQuery query)
    {
        _setOperations.Add((" INTERSECT ", query.GetSql()));
        return this;
    }

    public IClickHouseSelectBuilder<TEntity> Except(IWhereSubQuery query)
    {
        _setOperations.Add((" EXCEPT ", query.GetSql()));
        return this;
    }

    public IClickHouseSelectBuilder<TEntity> AddSql(string sqlString)
    {
        _setOperations.Add(("", sqlString));
        return this;
    }

    public IClickHouseSelectBuilder<TEntity> With<TSub>(string alias, Action<IClickHouseBuilder<TSub>> action)
    {
        if (string.IsNullOrWhiteSpace(alias))
            throw new ArgumentException("CTE alias is required.", nameof(alias));
        if (action == null)
            throw new ArgumentNullException(nameof(action));

        _commonTableExpressions.Add((false, alias, (cfg, dict) =>
        {
            var sub = new ClickHouseBuilder<TSub>(cfg);
            sub.BindParametersFrom(dict);
            action(sub);
            return sub.GetSql();
        }));
        return this;
    }

    public IClickHouseSelectBuilder<TEntity> WithRecursive(string alias, string innerSelectSql)
    {
        if (string.IsNullOrWhiteSpace(alias))
            throw new ArgumentException("CTE alias is required.", nameof(alias));
        if (innerSelectSql == null)
            throw new ArgumentNullException(nameof(innerSelectSql));

        _commonTableExpressions.Add((true, alias, (_, _) => innerSelectSql));
        return this;
    }

    public IClickHouseSelectBuilder<TEntity> FromCte(string cteName, string alias = null)
    {
        if (string.IsNullOrWhiteSpace(cteName))
            throw new ArgumentException("CTE name is required.", nameof(cteName));

        _fromTable = new Table("", cteName, alias);
        return this;
    }

    public override SqlQuery ToSql()
    {
        _currentParameters = new Dictionary<string, object>();
        _paramCounter = 0;
        return new SqlQuery
        {
            Sql = GetSqlInternal(true),
            Parameters = _currentParameters
        };
    }

    public override string ToDebugSql()
    {
        _currentParameters = null;
        _paramCounter = 0;
        return GetSqlInternal(true);
    }

    public override string GetSql()
    {
        return GetSqlInternal(false);
    }

    protected string GetSqlInternal(bool withEnd)
    {
        _builder = new ClickHouseSqlBuilder();
        var withPrefix = BuildCommonTableExpressionPrefix();
        if (withPrefix != null)
            _builder.AddSql(withPrefix);

        IClassMapper obj = _config.GetMap(typeof(TEntity));
        var table = new Table(obj.TableName);

        if (_selectPredicates.Count == 0)
            RecursiveParseSelect<TEntity>();

        Action<IClickHouseSelect> selectResult = null;
        foreach (ref Action<IClickHouseSelect> prev in CollectionsMarshal.AsSpan(_selectPredicates))
            if (selectResult == null)
                selectResult = prev;
            else
                selectResult += prev;

        _builder.Select(selectResult);
        _builder.From(_fromTable ?? table);

        if (_joinPredicates.Count != 0)
            foreach (var item in CollectionsMarshal.AsSpan(_joinPredicates))
                _builder.Join(item.Invoke());

        if (_isFinal) _builder.Final();

        if (_wherePredicates.Count != 0)
        {
            var group = new ClickHouseWherePredicate();
            foreach (var action in _wherePredicates)
                action(group);

            if (!group.IsEmpty())
            {
                var whereSql = group.ToSql();
                if (_wherePredicates.Count == 1 && whereSql.StartsWith("(") && whereSql.EndsWith(")"))
                    whereSql = whereSql.Substring(1, whereSql.Length - 2);
                _builder.Where(whereSql);
            }
        }

        if (_groupByPredicates.Count != 0)
        {
            Action<ClickHouseGroupByItem> groupByResult = null;
            foreach (var prev in _groupByPredicates)
                if (groupByResult == null)
                    groupByResult = prev;
                else
                    groupByResult += prev;

            _builder.GroupBy(groupByResult);
        }

        if (_orderPredicates.Count != 0)
        {
            Action<ClickHouseOrder> selectOrderResult = null;
            foreach (ref Action<ClickHouseOrder> prev in CollectionsMarshal.AsSpan(_orderPredicates))
                if (selectOrderResult == null)
                    selectOrderResult = prev;
                else
                    selectOrderResult += prev;

            _builder.OrderBy(selectOrderResult);
        }

        if (_havingPredicates != null)
            _builder.Having(_havingPredicates);

        if (_limit.HasValue)
            _builder.Limit(_limit.Value);

        if (_offset.HasValue)
            _builder.Offset(_offset.Value);

        foreach (var setOp in _setOperations)
        {
            if (!string.IsNullOrEmpty(setOp.op)) _builder.AddSql(setOp.op);
            _builder.AddSql(setOp.sql);
        }

        return withEnd ? _builder.Build() : _builder.BuildWithoutEnd();
    }

    private string BuildCommonTableExpressionPrefix()
    {
        if (_commonTableExpressions.Count == 0)
            return null;

        var anyRecursive = false;
        var parts = new List<string>(_commonTableExpressions.Count);
        foreach (var (recursive, alias, build) in _commonTableExpressions)
        {
            if (recursive)
                anyRecursive = true;

            var inner = NormalizeCteInnerSql(build(_config, _currentParameters));
            parts.Add($"{QuoteCteName(alias)} AS ({inner})");
        }

        return (anyRecursive ? "WITH RECURSIVE " : "WITH ") + string.Join(", ", parts) + " ";
    }

    private static string NormalizeCteInnerSql(string sql)
    {
        if (string.IsNullOrEmpty(sql))
            return sql;
        return sql.TrimEnd().TrimEnd(';').TrimEnd();
    }

    private string QuoteCteName(string name) => "\"" + EscapeIdentifier(name) + "\"";

    protected void RecursiveParseSelect<T>()
    {
        IClassMapper map = _config.GetMap(typeof(T));
        var table = new Table(map.TableName);

        foreach (PropertyMap item in map.Properties)
            _selectPredicates.Add(select =>
                select.Column(new Column(table.Name, item.ColumnName, item.Name)));
    }
}
