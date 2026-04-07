using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Globalization;
using System.Linq;
using Velox.Sql.Core.ClickHouseSql;
using Velox.Sql.Core.ClickHouseSql.Where;
using Velox.Sql.Core.Impl;
using Velox.Sql.Core.Interfaces;
using Velox.Sql.Expressions;
using Velox.Sql.Impl;
using Velox.Sql.Impl.Map;
using Velox.Sql.Interfaces;

namespace Velox.Sql.Impl.Builders.ClickHouse;

public abstract class ClickHouseBuilderBase<TEntity> : SqlBuilderCore<TEntity>
{
    protected readonly ClickHouseSqlConfiguration _config;
    protected ClickHouseSqlBuilder _builder;

    protected ClickHouseBuilderBase(ClickHouseSqlConfiguration config, ClickHouseSqlBuilder builder)
    {
        _config = config;
        _builder = builder;
    }

    public override void Dispose()
    {
        _builder = null;
    }

    protected string EscapeIdentifier(string name) => name.Replace("\"", "\"\"");

    protected IValue ConvertTo(object value)
    {
        if (value == null || value.ToString() == "null")
            return new ClickHouseValue();

        if (value is DateTime dt)
            return new ClickHouseValue(dt.ToString("yyyy-MM-dd HH:mm:ss"), false);

        if (_currentParameters != null)
        {
            var paramName = "p" + _currentParameters.Count;
            _currentParameters.Add(paramName, value);
            return new ClickHouseValue("@" + paramName, false);
        }

        if (value is bool b)
            return new ClickHouseValue(b ? "true" : "false", false);

        if (value is int i) return new ClickHouseValue(i.ToString(), false);
        if (value is long l) return new ClickHouseValue(l.ToString(), false);
        if (value is short sh) return new ClickHouseValue(sh.ToString(), false);
        if (value is decimal d) return new ClickHouseValue(d.ToString(CultureInfo.InvariantCulture), false);
        if (value is float f) return new ClickHouseValue(f.ToString(CultureInfo.InvariantCulture), false);
        if (value is double db) return new ClickHouseValue(db.ToString(CultureInfo.InvariantCulture), false);

        return new ClickHouseValue(value.ToString(), true);
    }

    internal ClickHouseValue ConvertExpressionValueToClickHouseValue(ExpressionInfoEx item)
    {
        if (item.IsFunction && item.FunctionName == "Contains")
        {
            var val = item.FunctionParams[0].Value?.ToString() ?? "";
            return (ClickHouseValue)ConvertTo($"%{val}%");
        }

        if (_currentParameters != null && item.RightOperatorValue != null)
        {
            var paramName = "p" + _currentParameters.Count;
            _currentParameters.Add(paramName, item.RightOperatorValue);
            return new ClickHouseValue("@" + paramName, false);
        }

        string value = item.RightOperatorName;

        if (DateTime.TryParseExact(value, "dd.MM.yyyy H:mm:ss", CultureInfo.InvariantCulture, DateTimeStyles.None,
                out DateTime date))
            return new ClickHouseValue(date.ToString("yyyy-MM-dd HH:mm:ss"), true);

        if (DateTime.TryParseExact(value, "dd.MM.yyyy", CultureInfo.InvariantCulture, DateTimeStyles.None,
                out DateTime dateOnly))
            return new ClickHouseValue(dateOnly.ToString("yyyy-MM-dd"), true);

        if (item.RightOperatorValue != null && item.RightOperatorValue.GetType().IsEnum)
            return new ClickHouseValue(((int)item.RightOperatorValue).ToString(), false);

        if (item.RightOperatorValue is bool b)
            return new ClickHouseValue(b ? "true" : "false", false);

        if (double.TryParse(value, NumberStyles.Any, CultureInfo.InvariantCulture, out double d))
            return new ClickHouseValue(d.ToString(CultureInfo.InvariantCulture), false);

        return new ClickHouseValue(value, true);
    }

    protected ClickHouseValue ConvertValueToClickHouseValue(Type type, object value, string valueStr, bool isRaw)
    {
        if (value == null || valueStr == "null")
            return new ClickHouseValue("null", false);

        if (_currentParameters != null)
        {
            var paramName = "p" + _currentParameters.Count;
            _currentParameters.Add(paramName, value);
            return new ClickHouseValue("@" + paramName, false);
        }

        if (type == typeof(string))
            return new ClickHouseValue(valueStr, true);

        if (type == typeof(DateTime))
            return new ClickHouseValue(((DateTime)value).ToString("yyyy-MM-dd HH:mm:ss"), true);

        if (type == typeof(bool))
            return new ClickHouseValue((bool)value ? "1" : "0", false);

        return new ClickHouseValue(valueStr, false);
    }

    internal ClickHouseWherePredicate ParseWhereExpression(ClickHouseWherePredicate group, ExpressionItemEx item, IClassMapper map)
    {
        if (item == null)
            return group;

        if (item.LeftExp != null)
        {
            if (item.LeftExp.IsFunction)
            {
                var value = ConvertExpressionValueToClickHouseValue(item.LeftExp);
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
                    var value = ConvertExpressionValueToClickHouseValue(item.LeftExp);
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
                var value = ConvertExpressionValueToClickHouseValue(item.RightExp);
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
                    var value = ConvertExpressionValueToClickHouseValue(item.RightExp);
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
            group.Grouping(x => ParseWhereExpression((ClickHouseWherePredicate)x, item.LeftNext, map));

            if (item.Type == ExpressionType.AndAlso)
                group.And();
            else
                group.Or();

            group.Grouping(x => ParseWhereExpression((ClickHouseWherePredicate)x, item.RightNext, map));

            return group;
        }

        if (item.LeftNext != null)
        {
            group.Grouping(x => ParseWhereExpression((ClickHouseWherePredicate)x, item.LeftNext, map));
            return group;
        }

        if (item.RightNext != null)
        {
            group.Grouping(x => ParseWhereExpression((ClickHouseWherePredicate)x, item.RightNext, map));
            return group;
        }

        return group;
    }
}
