using System;
using System.Collections.Generic;
using System.Text;
using Velox.Sql.Core.Impl;
using Velox.Sql.Core.Interfaces;
using Velox.Sql.Core.PostgreSql.Where;

namespace Velox.Sql.Core.ClickHouseSql.Where;

public sealed class ClickHouseWherePredicate : IWhere, IPredicate
{
    private readonly StringBuilder _sql;
    private bool _isLogicCommandDetected;

    public ClickHouseWherePredicate()
    {
        _sql = new StringBuilder();
        _isLogicCommandDetected = true;
    }

    public ClickHouseWherePredicate(string sql)
    {
        _sql = new StringBuilder(sql);
        _isLogicCommandDetected = true;
    }

    public override string ToString()
    {
        return _sql.ToString();
    }

    public string ToSql() => ToString();

    public IWhere And()
    {
        _isLogicCommandDetected = true;
        _sql.Append(SqlHelper.And);
        return this;
    }

    public IWhere Or()
    {
        _isLogicCommandDetected = true;
        _sql.Append(SqlHelper.Or);
        return this;
    }

    public IPredicate IsFalse(ITable table, string columnName)
    {
        if (!_isLogicCommandDetected) _sql.Append(SqlHelper.And);
        _isLogicCommandDetected = false;
        _sql.AppendTable(table).Append('.').AppendIdentifier(columnName).Append(" = 'FALSE'");
        return this;
    }

    public IPredicate IsNotNull(ITable table, string columnName)
    {
        if (!_isLogicCommandDetected) _sql.Append(SqlHelper.And);
        _isLogicCommandDetected = false;
        _sql.AppendTable(table).Append('.').AppendIdentifier(columnName).Append(" IS NOT NULL");
        return this;
    }

    public IPredicate IsNull(ITable table, string columnName)
    {
        if (!_isLogicCommandDetected) _sql.Append(SqlHelper.And);
        _isLogicCommandDetected = false;
        _sql.AppendTable(table).Append('.').AppendIdentifier(columnName).Append(" IS NULL");
        return this;
    }

    public IPredicate IsTrue(ITable table, string columnName)
    {
        if (!_isLogicCommandDetected) _sql.Append(SqlHelper.And);
        _isLogicCommandDetected = false;
        _sql.AppendTable(table).Append('.').AppendIdentifier(columnName).Append(" = 'TRUE'");
        return this;
    }

    public IPredicate Like(ITable table, string columnName, IValue value, bool isNot = false)
    {
        if (!_isLogicCommandDetected) _sql.Append(SqlHelper.And);
        _sql.AppendTable(table).Append('.').AppendIdentifier(columnName);
        if (isNot) _sql.Append(" NOT LIKE "); else _sql.Append(" LIKE ");
        _sql.Append(value);
        _isLogicCommandDetected = false;
        return this;
    }

    public IPredicate ILike(ITable table, string columnName, IValue value, bool isNot = false)
    {
        if (!_isLogicCommandDetected) _sql.Append(SqlHelper.And);
        _sql.AppendTable(table).Append('.').AppendIdentifier(columnName);
        if (isNot) _sql.Append(" NOT ILIKE "); else _sql.Append(" ILIKE ");
        _sql.Append(value);
        _isLogicCommandDetected = false;
        return this;
    }

    public IPredicate Not(ITable table, string columnName, IValue value = null)
    {
        if (!_isLogicCommandDetected) _sql.Append(SqlHelper.And);
        _isLogicCommandDetected = false;
        _sql.Append("NOT ").AppendTable(table).Append('.').AppendIdentifier(columnName);
        if (value != null) _sql.Append(" = ").Append(value);
        return this;
    }

    public IPredicate Any<TBuilder>(ITable table, string columnName, Operators @operator, ISqlBuilder<TBuilder> subQuery)
    {
        if (!_isLogicCommandDetected) _sql.Append(SqlHelper.And);
        _isLogicCommandDetected = false;
        _sql.AppendTable(table).Append('.').AppendIdentifier(columnName).Append(SqlHelper.ToString(@operator)).Append("ANY (").Append(subQuery.BuildWithoutEnd()).Append(')');
        return this;
    }

    public IPredicate All<TBuilder>(ITable table, string columnName, Operators @operator, ISqlBuilder<TBuilder> subQuery)
    {
        if (!_isLogicCommandDetected) _sql.Append(SqlHelper.And);
        _isLogicCommandDetected = false;
        _sql.AppendTable(table).Append('.').AppendIdentifier(columnName).Append(SqlHelper.ToString(@operator)).Append("ALL (").Append(subQuery.BuildWithoutEnd()).Append(')');
        return this;
    }

    public IPredicate Some<TBuilder>(ITable table, string columnName, Operators @operator, ISqlBuilder<TBuilder> subQuery)
    {
        if (!_isLogicCommandDetected) _sql.Append(SqlHelper.And);
        _isLogicCommandDetected = false;
        _sql.AppendTable(table).Append('.').AppendIdentifier(columnName).Append(SqlHelper.ToString(@operator)).Append("SOME (").Append(subQuery.BuildWithoutEnd()).Append(')');
        return this;
    }

    public IPredicate Exists<TBuilder>(ISqlBuilder<TBuilder> subQuery)
    {
        if (!_isLogicCommandDetected) _sql.Append(SqlHelper.And);
        _isLogicCommandDetected = false;
        _sql.Append("EXISTS (").Append(subQuery.BuildWithoutEnd()).Append(')');
        return this;
    }

    public IPredicate NotExists<TBuilder>(ISqlBuilder<TBuilder> subQuery)
    {
        if (!_isLogicCommandDetected) _sql.Append(SqlHelper.And);
        _isLogicCommandDetected = false;
        _sql.Append("NOT EXISTS (").Append(subQuery.BuildWithoutEnd()).Append(')');
        return this;
    }

    public IPredicate SetValue(ITable table, string columnName, Operators @operator, IValue value)
    {
        if (!_isLogicCommandDetected) _sql.Append(SqlHelper.And);
        _isLogicCommandDetected = false;
        _sql.AppendTable(table).Append('.').AppendIdentifier(columnName).Append(SqlHelper.ToString(@operator)).Append(value);
        return this;
    }

    public IPredicate Between(ITable table, string columnName, IValue firstValue, IValue secondValue)
    {
        if (!_isLogicCommandDetected) _sql.Append(SqlHelper.And);
        _isLogicCommandDetected = false;
        _sql.AppendTable(table).Append('.').AppendIdentifier(columnName).Append(" BETWEEN ").Append(firstValue).Append(" AND ").Append(secondValue);
        return this;
    }

    public IPredicate In(ITable table, string columnName, List<IValue> values, bool isNot = false)
    {
        if (!_isLogicCommandDetected) _sql.Append(SqlHelper.And);
        _isLogicCommandDetected = false;
        _sql.AppendTable(table).Append('.').AppendIdentifier(columnName);
        if (isNot) _sql.Append(" NOT");
        _sql.Append(" IN (").Append(string.Join(",", values)).Append(')');
        return this;
    }

    public IPredicate In<TBuilder>(ITable table, string columnName, ISqlBuilder<TBuilder> select)
    {
        if (!_isLogicCommandDetected) _sql.Append(SqlHelper.And);
        _isLogicCommandDetected = false;
        _sql.AppendTable(table).Append('.').AppendIdentifier(columnName).Append(" IN (").Append(select.BuildWithoutEnd()).Append(')');
        return this;
    }

    public IPredicate Grouping(Action<IWhere> predicate)
    {
        var tempPredicate = new ClickHouseWherePredicate();
        predicate(tempPredicate);
        _sql.Append('(').Append(tempPredicate.ToSql()).Append(')');
        return this;
    }

    public Predicate GroupingEx(Action<IPredicate> predicate)
    {
        throw new NotImplementedException("GroupingEx is not implemented for ClickHouse yet.");
    }

    public IPredicate Append(IPredicate expr)
    {
        var sql = (expr as IWhere)?.ToSql();
        if (string.IsNullOrEmpty(sql)) return this;
        if (!_isLogicCommandDetected) _sql.Append(SqlHelper.And);
        _isLogicCommandDetected = false;
        _sql.Append(sql);
        return this;
    }

    public IPredicate Append(string expr)
    {
        if (string.IsNullOrEmpty(expr)) return this;
        if (!_isLogicCommandDetected) _sql.Append(SqlHelper.And);
        _isLogicCommandDetected = false;
        _sql.Append(expr);
        return this;
    }

    public bool IsEmpty() => _sql.Length == 0;
}