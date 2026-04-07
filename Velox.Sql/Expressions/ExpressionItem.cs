using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using Velox.Sql.Core.Impl;

namespace Velox.Sql.Expressions;

internal sealed class ExpressionItemEx
{
    public ExpressionInfoEx LeftExp { get; set; }
    public ExpressionInfoEx RightExp { get; set; }
    public ExpressionType Type { get; set; }
    public ExpressionItemEx LeftNext { get; set; }
    public ExpressionItemEx RightNext { get; set; }
    public int OrderIndex { get; set; }
}

internal sealed class ExpressionInfoEx
{
    public Type GlobalType { get; set; }
    public string LeftOperatorName { get; set; }
    public Operators Operators { get; set; }
    public string RightOperatorName { get; set; }
    public object RightOperatorValue { get; set; }
    public Type ExpressionType { get; set; }
    public bool IsFunction { get; set; }
    public string FunctionName { get; set; }
    public List<ExpressionFunctionArgumentsEx> FunctionParams { get; set; } = new();

}

internal sealed class ExpressionFunctionArgumentsEx
{
    public Type Type { get; set; }
    public object Value { get; set; }
}