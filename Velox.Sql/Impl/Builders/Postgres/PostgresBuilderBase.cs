using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq.Expressions;
using Velox.Sql.Core.Impl;
using Velox.Sql.Core.Interfaces;
using Velox.Sql.Core.PostgreSql;
using Velox.Sql.Core.PostgreSql.Where;
using Velox.Sql.Expressions;
using Velox.Sql.Impl.Map;

namespace Velox.Sql.Impl.Builders.Postgres;

public abstract class PostgresBuilderBase<TEntity> : SqlBuilderCore<TEntity>
{
    protected readonly PgSqlConfiguration _config;
    protected PostgreSqlBuilder _builder;

    protected PostgresBuilderBase(PgSqlConfiguration config, PostgreSqlBuilder builder)
    {
        _config = config;
        _builder = builder;
    }

    /// <summary>
    /// Shares the parent's parameter dictionary so nested CTE subqueries participate in one numbered parameter list.
    /// </summary>
    internal void BindParametersFrom(Dictionary<string, object> parameters)
    {
        _currentParameters = parameters;
    }

    public override void Dispose()
    {
        _builder = null;
    }

    protected string EscapeIdentifier(string name) => name.Replace("\"", "\"\"");

    protected IValue ConvertTo(object value)
    {
        if (value == null || value.ToString() == "null")
            return new Value();

        ThrowIfUnsignedClrTypeNotSupportedForPostgres(value);

        var valueType = value.GetType();
        if (valueType.IsEnum)
            return ConvertTo(Convert.ChangeType(value, Enum.GetUnderlyingType(valueType)));

        if (value is DateTime dt)
            return new Value(dt);

        if (_currentParameters != null)
        {
            var paramName = "p" + _currentParameters.Count;
            _currentParameters.Add(paramName, value);
            return new Value(paramName, true, false);
        }

        if (value is bool b)
            return new Value(b.ToString().ToUpper(), false, true);

        if (value is int i) return new Value(i);
        if (value is long l) return new Value(l.ToString(), false, true);
        if (value is short sh) return new Value((int)sh);
        if (value is byte by) return new Value((int)by);
        if (value is sbyte sb) return new Value((int)sb);
        if (value is decimal d) return new Value(d.ToString(CultureInfo.InvariantCulture), false, true);
        if (value is float f) return new Value(f.ToString(CultureInfo.InvariantCulture), false, true);
        if (value is double db) return new Value(db.ToString(CultureInfo.InvariantCulture), false, true);

        return new Value(value.ToString(), false, false);
    }

    /// <summary>
    /// PostgreSQL has no matching unsigned integer types; Velox does not coerce CLR unsigned types to signed ones.
    /// </summary>
    private static void ThrowIfUnsignedClrTypeNotSupportedForPostgres(object value)
    {
        switch (value)
        {
            case ushort:
            case uint:
            case ulong:
            case UIntPtr: // nuint (native-sized unsigned)
            case UInt128:
                throw new NotSupportedException(
                    $"The CLR type '{value.GetType().Name}' is not supported for PostgreSQL in Velox. " +
                    "PostgreSQL does not provide unsigned integer types (including nuint/UIntPtr and UInt128); use a signed type that fits your column (short, int, long, or numeric) or adjust your model.");
        }
    }

    internal IValue ConvertExpressionValueToIValue(ExpressionInfoEx item)
    {
        if (item.IsFunction && item.FunctionName == "Contains")
        {
            var val = item.FunctionParams[0].Value?.ToString() ?? "";
            return ConvertTo($"%{val}%");
        }

        if (item.RightOperatorName == "null")
            return new Value();

        if (item.RightOperatorValue != null)
            return ConvertTo(item.RightOperatorValue);

        string value = item.RightOperatorName;

        if (DateTime.TryParseExact(value, "dd.MM.yyyy H:mm:ss", CultureInfo.InvariantCulture, DateTimeStyles.None, out DateTime date))
            return new Value(date);

        return ConvertTo(value);
    }

    internal Predicate ParseWhereExpression(Predicate group, ExpressionItemEx item, IClassMapper map)
    {
        if (item == null)
            return group;

        if (item.LeftExp != null)
        {
            if (item.LeftExp.IsFunction)
            {
                IValue value = ConvertExpressionValueToIValue(item.LeftExp);
                group.ILike(_config.GetTable(map.EntityType),
                    EscapeIdentifier(map.GetUserDefinedName(item.LeftExp.LeftOperatorName)), value);
            }
            else
            {
                if (item.LeftExp.RightOperatorValue == null)
                {
                    if (item.LeftExp.Operators == Operators.Equal)
                        group.IsNull(_config.GetTable(map.EntityType),
                            EscapeIdentifier(map.GetUserDefinedName(item.LeftExp.LeftOperatorName)));
                    else
                        group.IsNotNull(_config.GetTable(map.EntityType),
                            EscapeIdentifier(map.GetUserDefinedName(item.LeftExp.LeftOperatorName)));
                }
                else
                {
                    IValue value = ConvertExpressionValueToIValue(item.LeftExp);
                    group.SetValue(_config.GetTable(map.EntityType),
                        EscapeIdentifier(map.GetUserDefinedName(item.LeftExp.LeftOperatorName)),
                        item.LeftExp.Operators, value);
                }
            }
        }

        if (item.LeftExp != null && item.RightExp != null)
        {
            if (item.Type == ExpressionType.AndAlso)
                group.And();
            else
                group.Or();
        }

        if (item.RightExp != null)
        {
            if (item.RightExp.IsFunction)
            {
                IValue value = ConvertExpressionValueToIValue(item.RightExp);
                group.ILike(_config.GetTable(map.EntityType),
                    EscapeIdentifier(map.GetUserDefinedName(item.RightExp.LeftOperatorName)), value);
            }
            else
            {
                if (item.RightExp.RightOperatorValue == null)
                {
                    if (item.RightExp.Operators == Operators.Equal)
                        group.IsNull(_config.GetTable(map.EntityType),
                            EscapeIdentifier(map.GetUserDefinedName(item.RightExp.LeftOperatorName)));
                    else
                        group.IsNotNull(_config.GetTable(map.EntityType),
                            EscapeIdentifier(map.GetUserDefinedName(item.RightExp.LeftOperatorName)));
                }
                else
                {
                    IValue value = ConvertExpressionValueToIValue(item.RightExp);
                    group.SetValue(_config.GetTable(map.EntityType),
                        EscapeIdentifier(map.GetUserDefinedName(item.RightExp.LeftOperatorName)),
                        item.RightExp.Operators, value);
                }
            }
        }

        if ((item.LeftExp != null || item.RightExp != null) && (item.LeftNext != null || item.RightNext != null))
        {
            if (item.Type == ExpressionType.AndAlso)
                group.And();
            else
                group.Or();
        }

        if (item.LeftNext != null && item.RightNext != null)
        {
            group.Grouping(x => ParseWhereExpression((Predicate)x, item.LeftNext, map));

            if (item.Type == ExpressionType.AndAlso)
                group.And();
            else
                group.Or();

            group.Grouping(x => ParseWhereExpression((Predicate)x, item.RightNext, map));

            return group;
        }

        if (item.LeftNext != null)
        {
            group.Grouping(x => ParseWhereExpression((Predicate)x, item.LeftNext, map));
            return group;
        }

        if (item.RightNext != null)
        {
            group.Grouping(x => ParseWhereExpression((Predicate)x, item.RightNext, map));
            return group;
        }

        return group;
    }
}
