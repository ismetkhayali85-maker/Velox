using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Runtime.InteropServices;
using Velox.Sql.Core.Impl;
using Velox.Sql.Core.Interfaces;
using Velox.Sql.Core.PostgreSql;
using Velox.Sql.Expressions;
using Velox.Sql.Impl.Map;
using Velox.Sql.Impl.Clauses;
using Velox.Sql.Interfaces;

namespace Velox.Sql.Impl.Builders.Postgres;

public class PostgresSelectBuilder<TEntity> : PostgresBuilderBase<TEntity>, IPostgresSelectBuilder<TEntity>
{
    protected readonly List<Action<IGroupBy>> _groupByPredicates = new();
    protected readonly List<Action<IOrder>> _orderPredicates = new();
    protected readonly List<Action<ISelect>> _selectedPredicates = new();
    protected readonly List<Action<IWhere>> _wherePredicates = new();
    protected readonly List<Func<string>> _joinPredicates = new();
    protected readonly List<(string op, string sql)> _setOperations = new();
    protected readonly List<(bool Recursive, string Alias, Func<PgSqlConfiguration, Dictionary<string, object>, string> Build)> _commonTableExpressions = new();
    protected Action<IHaving> _havingPredicates;
    
    protected ITable _fromTable;
    protected ulong? _limit;
    protected ulong? _offset;

    public PostgresSelectBuilder(PgSqlConfiguration config, PostgreSqlBuilder builder) 
        : base(config, builder)
    {
    }

    public IPostgresSelectBuilder<TEntity> Select(Action<ISelect> action)
    {
        _selectedPredicates.Add(action);
        return this;
    }

    public IPostgresSelectBuilder<TEntity> Select(Expression<Func<TEntity, object>> expr = null, string alias = null)
    {
        IClassMapper map = _config.GetMap(typeof(TEntity));
        var table = new Table(map.SchemaName, map.TableName);

        if (expr == null)
        {
            RecursiveParseSelect<TEntity>();
            return this;
        }

        MemberUnaryResult[] exprSettings = ExpressionParser.FindMemberUnaryExpression(expr);
        foreach (MemberUnaryResult exprSetting in exprSettings.AsSpan())
            _selectedPredicates.Add(x =>
                x.Column(new Column(table, map.GetUserDefinedName(exprSetting.Value), string.IsNullOrEmpty(alias) ? exprSetting.Value : alias)));

        return this;
    }

    public IPostgresSelectBuilder<TEntity> Select<T>(Expression<Func<T, object>> expr = null, string alias = null)
    {
        IClassMapper map = _config.GetMap(typeof(T));
        var table = new Table(map.SchemaName, map.TableName);
        if (expr == null)
        {
            RecursiveParseSelect<T>();
            return this;
        }

        MemberUnaryResult[] exprSettings = ExpressionParser.FindMemberUnaryExpression(expr);
        foreach (MemberUnaryResult exprSetting in exprSettings.AsSpan())
            _selectedPredicates.Add(x =>
                x.Column(new Column(table, map.GetUserDefinedName(exprSetting.Value), string.IsNullOrEmpty(alias) ? exprSetting.Value : alias)));

        return this;
    }

    public IPostgresSelectBuilder<TEntity> SelectAll<T>()
    {
        RecursiveParseSelect<T>();
        return this;
    }

    public IPostgresSelectBuilder<TEntity> From<T>(string alias = null)
    {
        IClassMapper map = _config.GetMap(typeof(T));
        _fromTable = new Table(map.SchemaName, map.TableName, alias);
        return this;
    }

    public IPostgresSelectBuilder<TEntity> Where<T>(Expression<Func<T, bool>> whereExpr)
    {
        if (whereExpr == null) return this;

        IClassMapper map = _config.GetMap(typeof(T));
        ExpressionResultEx exprSettings = ExpressionParser.FindExpressionEx(whereExpr);

        _wherePredicates.Add(w =>
        {
            var group = new Velox.Sql.Core.PostgreSql.Where.Predicate();
            ExpressionItemEx item = exprSettings.Settings;
            ParseWhereExpression(group, item, map);
            w.Append($"({group.ToSql()})");
        });

        return this;
    }

    public IPostgresSelectBuilder<TEntity> Where(Expression<Func<TEntity, bool>> whereExpr)
    {
        return Where<TEntity>(whereExpr);
    }

    public IPostgresSelectBuilder<TEntity> Where(Action<IWhere<TEntity>> action)
    {
        _wherePredicates.Add(w => 
        {
            action(new WhereClause<TEntity>(w, _config, ConvertTo));
        });
        return this;
    }

    public IPostgresSelectBuilder<TEntity> InnerJoin<TNewTable, TOldTable>(
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

            return $" INNER JOIN {tableFrom.Init()} ON {tableTo}.\"{EscapeIdentifier(toMap.GetUserDefinedName(toField.Value))}\" = {tableFrom}.\"{EscapeIdentifier(fromMap.GetUserDefinedName(fromField.Value))}\"";
        });

        if (resultSelector == null) return this;

        MemberUnaryResult[] exprSettings = ExpressionParser.FindMemberUnaryExpression(resultSelector);
        foreach (MemberUnaryResult exprSetting in exprSettings.AsSpan())
        {
            IClassMapper selectMap = _config.GetMap(exprSetting.Type);
            _selectedPredicates.Add(x =>
                x.Column(new Column(new Table(selectMap.SchemaName, selectMap.TableName), selectMap.GetUserDefinedName(exprSetting.Value), exprSetting.Value)));
        }

        return this;
    }

    public IPostgresSelectBuilder<TEntity> LeftJoin<TNewTable, TOldTable>(
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
            _selectedPredicates.Add(x =>
                x.Column(new Column(new Table(selectMap.SchemaName, selectMap.TableName), selectMap.GetUserDefinedName(exprSetting.Value), exprSetting.Value)));
        }

        return this;
    }

    public IPostgresSelectBuilder<TEntity> RightJoin<TNewTable, TOldTable>(
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
            _selectedPredicates.Add(x =>
                x.Column(new Column(new Table(selectMap.SchemaName, selectMap.TableName), selectMap.GetUserDefinedName(exprSetting.Value), exprSetting.Value)));
        }

        return this;
    }

    public IPostgresSelectBuilder<TEntity> FullJoin<TNewTable, TOldTable>(
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
            _selectedPredicates.Add(x =>
                x.Column(new Column(new Table(selectMap.SchemaName, selectMap.TableName), selectMap.GetUserDefinedName(exprSetting.Value), exprSetting.Value)));
        }

        return this;
    }

    public IPostgresSelectBuilder<TEntity> CrossJoin<TTable>()
    {
        _joinPredicates.Add(() => string.Concat(" CROSS JOIN ", _config.GetTable(typeof(TTable))));
        return this;
    }

    public IPostgresSelectBuilder<TEntity> GroupBy(Expression<Func<TEntity, object>> expression = null)
    {
        IClassMapper map = _config.GetMap(typeof(TEntity));
        var table = new Table(map.SchemaName, map.TableName);

        if (expression == null)
            _groupByPredicates.Add(x => x.Item(table, "*"));
        else
        {
            MemberUnaryResult[] exprSettings = ExpressionParser.FindMemberUnaryExpression(expression);
            foreach (MemberUnaryResult exprSetting in exprSettings.AsSpan())
                _groupByPredicates.Add(x => x.Item(table, map.GetUserDefinedName(exprSetting.Value)));
        }

        return this;
    }

    public IPostgresSelectBuilder<TEntity> Having(Action<IHaving<TEntity>> action)
    {
        if (_havingPredicates == null)
            _havingPredicates = h => action(new HavingClause<TEntity>(h, _config, ConvertTo));
        else
            _havingPredicates += h => action(new HavingClause<TEntity>(h, _config, ConvertTo));
        return this;
    }

    public IPostgresSelectBuilder<TEntity> OrderBy<T>(bool isAsc = true, Expression<Func<T, object>> expr = null, bool sortByAlias = false)
    {
        IClassMapper map = _config.GetMap(typeof(T));
        var table = new Table(map.SchemaName, map.TableName);

        if (expr != null)
        {
            MemberUnaryResult[] exprSettings = ExpressionParser.FindMemberUnaryExpression(expr);
            foreach (MemberUnaryResult exprSetting in exprSettings.AsSpan())
                if (sortByAlias)
                {
                    if (isAsc)
                        _orderPredicates.Add(order => order.AscByAlias(exprSetting.Value));
                    else
                        _orderPredicates.Add(order => order.DescByAlias(exprSetting.Value));
                }
                else if (isAsc)
                    _orderPredicates.Add(order =>
                        order.Asc(table, map.GetUserDefinedName(exprSetting.Value)));
                else
                    _orderPredicates.Add(order =>
                        order.Desc(table, map.GetUserDefinedName(exprSetting.Value)));
            return this;
        }

        if (isAsc)
            _orderPredicates.Add(order =>
                order.Asc(table, map.GetUserDefinedName(map.HasKey().ColumnName)));
        else
            _orderPredicates.Add(order =>
                order.Desc(table, map.GetUserDefinedName(map.HasKey().ColumnName)));

        return this;
    }

    public IPostgresSelectBuilder<TEntity> OrderBy(bool isAsc = true, Expression<Func<TEntity, object>> expr = null, bool sortByAlias = false)
    {
        return OrderBy<TEntity>(isAsc, expr, sortByAlias);
    }

    public IPostgresSelectBuilder<TEntity> OrderByAsc<T>(Expression<Func<T, object>> expr = null, bool sortByAlias = false) =>
        OrderBy<T>(true, expr, sortByAlias);

    public IPostgresSelectBuilder<TEntity> OrderByDesc<T>(Expression<Func<T, object>> expr = null, bool sortByAlias = false) =>
        OrderBy<T>(false, expr, sortByAlias);

    public IPostgresSelectBuilder<TEntity> OrderByAsc(Expression<Func<TEntity, object>> expr = null, bool sortByAlias = false) =>
        OrderBy<TEntity>(true, expr, sortByAlias);

    public IPostgresSelectBuilder<TEntity> OrderByDesc(Expression<Func<TEntity, object>> expr = null, bool sortByAlias = false) =>
        OrderBy<TEntity>(false, expr, sortByAlias);

    public IPostgresSelectBuilder<TEntity> Limit(uint value)
    {
        _limit = value;
        return this;
    }

    public IPostgresSelectBuilder<TEntity> Offset(uint value)
    {
        _offset = value;
        return this;
    }

    public IPostgresSelectBuilder<TEntity> Count(Expression<Func<TEntity, object>> expression = null, bool isDistinct = false, string alias = null)
    {
        IClassMapper map = _config.GetMap(typeof(TEntity));
        var table = new Table(map.SchemaName, map.TableName);

        if (expression == null)
        {
            _selectedPredicates.Add(x => x.CountAll());
        }
        else
        {
            MemberUnaryResult[] exprSettings = ExpressionParser.FindMemberUnaryExpression(expression);

            if (isDistinct)
                foreach (MemberUnaryResult exprSetting in exprSettings.AsSpan())
                    _selectedPredicates.Add(x =>
                        x.CountDistinct(new Column(table, map.GetUserDefinedName(exprSetting.Value),
                            string.IsNullOrEmpty(alias) ? exprSetting.Value : alias)));
            else
                foreach (MemberUnaryResult exprSetting in exprSettings.AsSpan())
                    _selectedPredicates.Add(x =>
                        x.Count(new Column(table, map.GetUserDefinedName(exprSetting.Value),
                            string.IsNullOrEmpty(alias) ? exprSetting.Value : alias)));
        }

        return this;
    }

    public IPostgresSelectBuilder<TEntity> Count(string alias) => Count(null, false, alias);
    public IPostgresSelectBuilder<TEntity> Count(Expression<Func<TEntity, object>> expr, string alias) => Count(expr, false, alias);
    public IPostgresSelectBuilder<TEntity> CountDistinct(Expression<Func<TEntity, object>> expression, string alias = null) => Count(expression, true, alias);

    public IPostgresSelectBuilder<TEntity> Distinct(Expression<Func<TEntity, object>> expression)
    {
        IClassMapper map = _config.GetMap(typeof(TEntity));
        var table = new Table(map.SchemaName, map.TableName);

        MemberUnaryResult[] exprSettings = ExpressionParser.FindMemberUnaryExpression(expression);
        foreach (MemberUnaryResult exprSetting in exprSettings.AsSpan())
            _selectedPredicates.Add(x =>
                x.Distinct(new Column(table, map.GetUserDefinedName(exprSetting.Value), exprSetting.Value)));

        return this;
    }

    public IPostgresSelectBuilder<TEntity> Sum(Expression<Func<TEntity, object>> expr, string alias = null)
    {
        IClassMapper map = _config.GetMap(typeof(TEntity));
        var table = new Table(map.SchemaName, map.TableName);

        MemberUnaryResult[] exprSettings = ExpressionParser.FindMemberUnaryExpression(expr);
        foreach (MemberUnaryResult exprSetting in exprSettings.AsSpan())
            _selectedPredicates.Add(x =>
                x.Function(new Column(table, map.GetUserDefinedName(exprSetting.Value), ""), "SUM", alias));

        return this;
    }

    public IPostgresSelectBuilder<TEntity> Avg(Expression<Func<TEntity, object>> expr, string alias = null)
    {
        IClassMapper map = _config.GetMap(typeof(TEntity));
        var table = new Table(map.SchemaName, map.TableName);

        MemberUnaryResult[] exprSettings = ExpressionParser.FindMemberUnaryExpression(expr);
        foreach (MemberUnaryResult exprSetting in exprSettings.AsSpan())
            _selectedPredicates.Add(x =>
                x.Function(new Column(table, map.GetUserDefinedName(exprSetting.Value), ""), "AVG", alias));

        return this;
    }

    public IPostgresSelectBuilder<TEntity> Min(Expression<Func<TEntity, object>> expr, string alias = null)
    {
        IClassMapper map = _config.GetMap(typeof(TEntity));
        var table = new Table(map.SchemaName, map.TableName);

        MemberUnaryResult[] exprSettings = ExpressionParser.FindMemberUnaryExpression(expr);
        foreach (MemberUnaryResult exprSetting in exprSettings.AsSpan())
            _selectedPredicates.Add(x =>
                x.Function(new Column(table, map.GetUserDefinedName(exprSetting.Value), ""), "MIN", alias));

        return this;
    }

    public IPostgresSelectBuilder<TEntity> Max(Expression<Func<TEntity, object>> expr, string alias = null)
    {
        IClassMapper map = _config.GetMap(typeof(TEntity));
        var table = new Table(map.SchemaName, map.TableName);

        MemberUnaryResult[] exprSettings = ExpressionParser.FindMemberUnaryExpression(expr);
        foreach (MemberUnaryResult exprSetting in exprSettings.AsSpan())
            _selectedPredicates.Add(x =>
                x.Function(new Column(table, map.GetUserDefinedName(exprSetting.Value), ""), "MAX", alias));

        return this;
    }

    public IPostgresSelectBuilder<TEntity> SumOver<TSum>(Expression<Func<TSum, object>> sumExpr,
        Action<PostgresWindowOverClause> window, string alias = null) =>
        FunctionOver(sumExpr, "SUM", window, alias);

    public IPostgresSelectBuilder<TEntity> AvgOver<TAvg>(Expression<Func<TAvg, object>> expr,
        Action<PostgresWindowOverClause> window, string alias = null) =>
        FunctionOver(expr, "AVG", window, alias);

    public IPostgresSelectBuilder<TEntity> MinOver<TMin>(Expression<Func<TMin, object>> expr,
        Action<PostgresWindowOverClause> window, string alias = null) =>
        FunctionOver(expr, "MIN", window, alias);

    public IPostgresSelectBuilder<TEntity> MaxOver<TMax>(Expression<Func<TMax, object>> expr,
        Action<PostgresWindowOverClause> window, string alias = null) =>
        FunctionOver(expr, "MAX", window, alias);

    public IPostgresSelectBuilder<TEntity> RowNumberOver(Action<PostgresWindowOverClause> window, string alias = "RowNumber")
    {
        _selectedPredicates.Add(s =>
        {
            var w = new PostgresWindowOverClause(_config);
            window(w);
            s.RowNumberOver(w.Build(), alias);
        });
        return this;
    }

    public IPostgresSelectBuilder<TEntity> CountOver(Action<PostgresWindowOverClause> window, string alias = null)
    {
        _selectedPredicates.Add(s =>
        {
            var w = new PostgresWindowOverClause(_config);
            window(w);
            s.CountAllOver(w.Build(), alias);
        });
        return this;
    }

    private IPostgresSelectBuilder<TEntity> FunctionOver<TCol>(Expression<Func<TCol, object>> columnExpr, string funcName,
        Action<PostgresWindowOverClause> window, string alias)
    {
        IClassMapper map = _config.GetMap(typeof(TCol));
        var table = new Table(map.SchemaName, map.TableName);
        MemberUnaryResult[] exprSettings = ExpressionParser.FindMemberUnaryExpression(columnExpr);
        foreach (MemberUnaryResult exprSetting in exprSettings.AsSpan())
        {
            var col = new Column(table, map.GetUserDefinedName(exprSetting.Value), "");
            _selectedPredicates.Add(s =>
            {
                var w = new PostgresWindowOverClause(_config);
                window(w);
                s.FunctionOver(col, funcName, w.Build(), string.IsNullOrEmpty(alias) ? exprSetting.Value : alias);
            });
        }

        return this;
    }

    public IPostgresSelectBuilder<TEntity> Union(IWhereSubQuery query)
    {
        _setOperations.Add((" UNION ", query.GetSql()));
        return this;
    }

    public IPostgresSelectBuilder<TEntity> UnionAll(IWhereSubQuery query)
    {
        _setOperations.Add((" UNION ALL ", query.GetSql()));
        return this;
    }

    public IPostgresSelectBuilder<TEntity> Intersect(IWhereSubQuery query)
    {
        _setOperations.Add((" INTERSECT ", query.GetSql()));
        return this;
    }

    public IPostgresSelectBuilder<TEntity> Except(IWhereSubQuery query)
    {
        _setOperations.Add((" EXCEPT ", query.GetSql()));
        return this;
    }

    public IPostgresSelectBuilder<TEntity> AddSql(string sqlString)
    {
        _setOperations.Add(("", sqlString));
        return this;
    }

    public IPostgresSelectBuilder<TEntity> SubQuery<TSub>(Action<IPostgresBuilder<TSub>> action, string alias = null)
    {
        var subBuilder = new PostgresBuilder<TSub>(_config);
        action(subBuilder);
        _selectedPredicates.Add(x => x.Expression($"{subBuilder.GetSql()}" + (string.IsNullOrEmpty(alias) ? "" : $" AS \"{alias}\"")));
        return this;
    }

    public IPostgresSelectBuilder<TEntity> With<TSub>(string alias, Action<IPostgresBuilder<TSub>> action)
    {
        if (string.IsNullOrWhiteSpace(alias))
            throw new ArgumentException("CTE alias is required.", nameof(alias));
        if (action == null)
            throw new ArgumentNullException(nameof(action));

        _commonTableExpressions.Add((false, alias, (cfg, dict) =>
        {
            var sub = new PostgresBuilder<TSub>(cfg);
            sub.BindParametersFrom(dict);
            action(sub);
            return sub.GetSql();
        }));
        return this;
    }

    public IPostgresSelectBuilder<TEntity> WithRecursive(string alias, string innerSelectSql)
    {
        if (string.IsNullOrWhiteSpace(alias))
            throw new ArgumentException("CTE alias is required.", nameof(alias));
        if (innerSelectSql == null)
            throw new ArgumentNullException(nameof(innerSelectSql));

        _commonTableExpressions.Add((true, alias, (_, _) => innerSelectSql));
        return this;
    }

    public IPostgresSelectBuilder<TEntity> FromCte(string cteName, string alias = null)
    {
        if (string.IsNullOrWhiteSpace(cteName))
            throw new ArgumentException("CTE name is required.", nameof(cteName));

        _fromTable = new Table("", cteName, alias);
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

    protected string GetSqlInternal(bool withEnd)
    {
        _builder = new PostgreSqlBuilder();
        var withPrefix = BuildCommonTableExpressionPrefix();
        if (withPrefix != null)
            _builder.AddSql(withPrefix);

        IClassMapper map = _config.GetMap(typeof(TEntity));

        if (_selectedPredicates.Count == 0)
            RecursiveParseSelect<TEntity>();

        Action<ISelect> selectResult = null;
        foreach (ref Action<ISelect> prev in CollectionsMarshal.AsSpan(_selectedPredicates))
            if (selectResult == null)
                selectResult = prev;
            else
                selectResult += prev;

        _builder.Select(selectResult);
        _builder.From(_fromTable ?? new Table(map.SchemaName, map.TableName));

        if (_joinPredicates.Count != 0)
            foreach (Func<string> item in CollectionsMarshal.AsSpan(_joinPredicates))
                _builder.Join(item.Invoke());

        if (_wherePredicates.Count != 0)
        {
            var group = new Velox.Sql.Core.PostgreSql.Where.Predicate();
            foreach (var action in _wherePredicates)
                action(group);

            if (!group.IsEmpty())
            {
                var sql = group.ToSql();
                if (_wherePredicates.Count == 1 && sql.StartsWith("(") && sql.EndsWith(")"))
                    sql = sql.Substring(1, sql.Length - 2);
                _builder.Where(sql);
            }
        }

        if (_groupByPredicates.Count != 0)
        {
            Action<IGroupBy> groupByResult = null;
            foreach (Action<IGroupBy> prev in CollectionsMarshal.AsSpan(_groupByPredicates))
                if (groupByResult == null)
                    groupByResult = prev;
                else
                    groupByResult += prev;

            _builder.GroupBy(groupByResult);
        }

        if (_orderPredicates.Count != 0)
        {
            Action<IOrder> selectOrderResult = null;
            foreach (ref Action<IOrder> prev in CollectionsMarshal.AsSpan(_orderPredicates))
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

        foreach ((string op, string setSql) in CollectionsMarshal.AsSpan(_setOperations))
        {
            if (!string.IsNullOrEmpty(op)) _builder.AddSql(op);
            _builder.AddSql(setSql);
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
        var table = new Table(map.SchemaName, map.TableName);

        var sortedProperties = map.Properties
            .OrderByDescending(x => x.KeyType != KeyType.NotAKey)
            .ThenByDescending(x => x.Name.Equals("id", StringComparison.OrdinalIgnoreCase))
            .ThenBy(x => x.Name);

        foreach (PropertyMap item in sortedProperties)
            _selectedPredicates.Add(select =>
                select.Column(new Column(table, item.ColumnName, item.Name)));
    }
}
