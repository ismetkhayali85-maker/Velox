using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using Velox.Sql.Core.Impl;
using Velox.Sql.Core.Interfaces;
using Velox.Sql.Expressions;

namespace Velox.Sql.Impl.Clauses;

public class WhereClause<TEntity> : IWhere<TEntity>
{
    private readonly IWhere _builder;
    private readonly ISqlConfiguration _config;
    private readonly Func<object, IValue> _converter;

    public WhereClause(IWhere builder, ISqlConfiguration config, Func<object, IValue> converter)
    {
        _builder = builder;
        _config = config;
        _converter = converter;
    }

    public IWhere<TEntity> And()
    {
        _builder.And();
        return this;
    }

    public IWhere<TEntity> Or()
    {
        _builder.Or();
        return this;
    }

    public IWhere<TEntity> IsFalse(Expression<Func<TEntity, object>> expr)
    {
        Resolve(expr, out var table, out var column);
        _builder.IsFalse(table, column);
        return this;
    }

    public IWhere<TEntity> IsNotNull(Expression<Func<TEntity, object>> expr)
    {
        Resolve(expr, out var table, out var column);
        _builder.IsNotNull(table, column);
        return this;
    }

    public IWhere<TEntity> IsNull(Expression<Func<TEntity, object>> expr)
    {
        Resolve(expr, out var table, out var column);
        _builder.IsNull(table, column);
        return this;
    }

    public IWhere<TEntity> IsTrue(Expression<Func<TEntity, object>> expr)
    {
        Resolve(expr, out var table, out var column);
        _builder.IsTrue(table, column);
        return this;
    }

    public IWhere<TEntity> Like(Expression<Func<TEntity, object>> expr, object value, bool isNot = false)
    {
        Resolve(expr, out var table, out var column);
        _builder.Like(table, column, _converter(value), isNot);
        return this;
    }

    public IWhere<TEntity> ILike(Expression<Func<TEntity, object>> expr, object value, bool isNot = false)
    {
        Resolve(expr, out var table, out var column);
        _builder.ILike(table, column, _converter(value), isNot);
        return this;
    }

    public IWhere<TEntity> Exists(IWhereSubQuery subQuery)
    {
        _builder.Exists(new RawSqlBuilder(subQuery.GetSql()));
        return this;
    }

    public IWhere<TEntity> NotExists(IWhereSubQuery subQuery)
    {
        _builder.NotExists(new RawSqlBuilder(subQuery.GetSql()));
        return this;
    }

    public IWhere<TEntity> SetValue(Expression<Func<TEntity, object>> expr, Operators @operator, object value)
    {
        Resolve(expr, out var table, out var column);
        _builder.SetValue(table, column, @operator, _converter(value));
        return this;
    }

    public IWhere<TEntity> Between(Expression<Func<TEntity, object>> expr, object firstValue, object secondValue)
    {
        Resolve(expr, out var table, out var column);
        _builder.Between(table, column, _converter(firstValue), _converter(secondValue));
        return this;
    }

    public IWhere<TEntity> In(Expression<Func<TEntity, object>> expr, IEnumerable<object> values, bool isNot = false)
    {
        Resolve(expr, out var table, out var column);
        var convertedValues = values.Select(v => _converter(v)).ToList();
        _builder.In(table, column, convertedValues, isNot);
        return this;
    }

    public IWhere<TEntity> In(Expression<Func<TEntity, object>> expr, IWhereSubQuery subQuery)
    {
        Resolve(expr, out var table, out var column);
        _builder.In(table, column, new RawSqlBuilder(subQuery.GetSql()));
        return this;
    }

    public IWhere<TEntity> Any(Expression<Func<TEntity, object>> expr, Operators @operator, IWhereSubQuery subQuery)
    {
        Resolve(expr, out var table, out var column);
        _builder.Any(table, column, @operator, new RawSqlBuilder(subQuery.GetSql()));
        return this;
    }

    public IWhere<TEntity> All(Expression<Func<TEntity, object>> expr, Operators @operator, IWhereSubQuery subQuery)
    {
        Resolve(expr, out var table, out var column);
        _builder.All(table, column, @operator, new RawSqlBuilder(subQuery.GetSql()));
        return this;
    }

    public IWhere<TEntity> Some(Expression<Func<TEntity, object>> expr, Operators @operator, IWhereSubQuery subQuery)
    {
        Resolve(expr, out var table, out var column);
        _builder.Some(table, column, @operator, new RawSqlBuilder(subQuery.GetSql()));
        return this;
    }

    private void Resolve(Expression<Func<TEntity, object>> expr, out ITable table, out string column)
    {
        var members = ExpressionParser.FindMemberUnaryExpression(expr);
        var member = members.First();
        var map = _config.GetMap(typeof(TEntity));
        table = _config.GetTable(typeof(TEntity));
        column = map.GetUserDefinedName(member.Value);
    }
}

internal class RawSqlBuilder : ISqlBuilder<RawSqlBuilder>
{
    private readonly string _sql;

    public RawSqlBuilder(string sql) => _sql = sql;

    public RawSqlBuilder AddSql(string sql) => throw new NotImplementedException();
    public RawSqlBuilder Comments(string commentText) => throw new NotImplementedException();
    public RawSqlBuilder Select() => throw new NotImplementedException();
    public RawSqlBuilder Select(Action<ISelect> action) => throw new NotImplementedException();
    public RawSqlBuilder SelectInto(ITable whichITable, Action<ISelect> action) => throw new NotImplementedException();
    public RawSqlBuilder Delete() => throw new NotImplementedException();
    public RawSqlBuilder Truncate(ITable table) => throw new NotImplementedException();
    public RawSqlBuilder From(ITable table) => throw new NotImplementedException();
    public RawSqlBuilder From(List<ITable> tables) => throw new NotImplementedException();
    public RawSqlBuilder Where(Action<IWhere> action) => throw new NotImplementedException();
    public RawSqlBuilder OrderBy(Action<IOrder> orders) => throw new NotImplementedException();
    public RawSqlBuilder Offset(ulong value) => throw new NotImplementedException();
    public RawSqlBuilder Join<T>(IJoin<T> value) => throw new NotImplementedException();
    public RawSqlBuilder Insert(ITable table, Action<IInsert> action) => throw new NotImplementedException();
    public RawSqlBuilder Insert(ITable table) => throw new NotImplementedException();
    public RawSqlBuilder Update(ITable table, Action<IUpdate> action) => throw new NotImplementedException();
    public RawSqlBuilder GroupBy(Action<IGroupBy> items) => throw new NotImplementedException();
    public RawSqlBuilder Having(Action<IHaving> columns) => throw new NotImplementedException();
    public string Build() => _sql;
    public string BuildWithoutEnd() => _sql;
}
