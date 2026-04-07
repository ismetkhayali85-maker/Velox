using System.Text;
using Velox.Sql.Core.Impl;
using Velox.Sql.Core.Interfaces;

namespace Velox.Sql.Core.ClickHouseSql.Having;

public sealed class ClickHouseHaving : IHaving
{
    private readonly StringBuilder _sql;
    private bool _isLogicCommandDetected;

    public ClickHouseHaving()
    {
        _sql = new StringBuilder();
        _isLogicCommandDetected = true;
    }

    public IHaving And()
    {
        _sql.Append(SqlHelper.And);
        _isLogicCommandDetected = true;
        return this;
    }

    public IHaving Or()
    {
        _sql.Append(SqlHelper.Or);
        _isLogicCommandDetected = true;
        return this;
    }

    public override string ToString()
    {
        return _sql.ToString();
    }

    public IHaving SetValue(ITable table, string column, Operators @operator, IValue value)
    {
        if (!_isLogicCommandDetected) _sql.Append(SqlHelper.And);
        _isLogicCommandDetected = false;
        _sql.AppendTable(table)
            .Append('.')
            .AppendIdentifier(column)
            .Append(SqlHelper.ToString(@operator))
            .Append(value);
        return this;
    }

    public IHaving Count(ITable table, string column, Operators @operator, IValue value)
    {
        return Aggregate("count", table, column, @operator, value);
    }

    public IHaving Sum(ITable table, string column, Operators @operator, IValue value)
    {
        return Aggregate("sum", table, column, @operator, value);
    }

    public IHaving Avg(ITable table, string column, Operators @operator, IValue value)
    {
        return Aggregate("avg", table, column, @operator, value);
    }

    public IHaving Min(ITable table, string column, Operators @operator, IValue value)
    {
        return Aggregate("min", table, column, @operator, value);
    }

    public IHaving Max(ITable table, string column, Operators @operator, IValue value)
    {
        return Aggregate("max", table, column, @operator, value);
    }

    public IHaving CountDistinct(ITable table, string column, Operators @operator, IValue value)
    {
        if (!_isLogicCommandDetected) _sql.Append(SqlHelper.And);
        _isLogicCommandDetected = false;
        _sql.Append("uniq(")
            .AppendTable(table)
            .Append('.')
            .AppendIdentifier(column)
            .Append(')')
            .Append(SqlHelper.ToString(@operator))
            .Append(value);
        return this;
    }

    public IHaving IsTrue(ITable table, string column)
    {
        if (!_isLogicCommandDetected) _sql.Append(SqlHelper.And);
        _isLogicCommandDetected = false;
        _sql.AppendTable(table).Append('.').AppendIdentifier(column).Append(" = 'TRUE'");
        return this;
    }

    public IHaving IsFalse(ITable table, string column)
    {
        if (!_isLogicCommandDetected) _sql.Append(SqlHelper.And);
        _isLogicCommandDetected = false;
        _sql.AppendTable(table).Append('.').AppendIdentifier(column).Append(" = 'FALSE'");
        return this;
    }

    public IHaving IsNull(ITable table, string column)
    {
        if (!_isLogicCommandDetected) _sql.Append(SqlHelper.And);
        _isLogicCommandDetected = false;
        _sql.AppendTable(table).Append('.').AppendIdentifier(column).Append(" IS NULL");
        return this;
    }

    public IHaving IsNotNull(ITable table, string column)
    {
        if (!_isLogicCommandDetected) _sql.Append(SqlHelper.And);
        _isLogicCommandDetected = false;
        _sql.AppendTable(table).Append('.').AppendIdentifier(column).Append(" IS NOT NULL");
        return this;
    }

    private IHaving Aggregate(string function, ITable table, string column, Operators @operator, IValue value)
    {
        if (!_isLogicCommandDetected) _sql.Append(SqlHelper.And);
        _isLogicCommandDetected = false;
        _sql.Append(function)
            .Append('(')
            .AppendTable(table)
            .Append('.')
            .AppendIdentifier(column)
            .Append(')')
            .Append(SqlHelper.ToString(@operator))
            .Append(value);
        return this;
    }

}
