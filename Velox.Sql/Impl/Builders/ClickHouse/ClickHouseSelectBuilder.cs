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
                        order.Asc(new Column(map.TableName, sortByAlias
                            ? exprSetting.Value
                            : map.GetUserDefinedName(exprSetting.Value), "")));
                else
                    _orderPredicates.Add(order =>
                        order.Desc(new Column(map.TableName, sortByAlias
                            ? exprSetting.Value
                            : map.GetUserDefinedName(exprSetting.Value), "")));
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

    public IClickHouseSelectBuilder<TEntity> Any(Expression<Func<TEntity, object>> action, string alias = null)
    {
        IClassMapper map = _config.GetMap(typeof(TEntity));
        MemberUnaryResult[] exprSettings = ExpressionParser.FindMemberUnaryExpression(action);
        foreach (MemberUnaryResult exprSetting in exprSettings.AsSpan())
            _selectPredicates.Add(x => x.Function(new Column(map.TableName, map.GetUserDefinedName(exprSetting.Value), ""), "any", string.IsNullOrEmpty(alias) ? exprSetting.Value : alias));
        return this;
    }

    public IClickHouseSelectBuilder<TEntity> AnyLast(Expression<Func<TEntity, object>> action, string alias = null)
    {
        IClassMapper map = _config.GetMap(typeof(TEntity));
        MemberUnaryResult[] exprSettings = ExpressionParser.FindMemberUnaryExpression(action);
        foreach (MemberUnaryResult exprSetting in exprSettings.AsSpan())
            _selectPredicates.Add(x => x.Function(new Column(map.TableName, map.GetUserDefinedName(exprSetting.Value), ""), "anyLast", string.IsNullOrEmpty(alias) ? exprSetting.Value : alias));
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

    protected void RecursiveParseSelect<T>()
    {
        IClassMapper map = _config.GetMap(typeof(T));
        var table = new Table(map.TableName);

        foreach (PropertyMap item in map.Properties)
            _selectPredicates.Add(select =>
                select.Column(new Column(table.Name, item.ColumnName, item.Name)));
    }
}
