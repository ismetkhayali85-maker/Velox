using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq.Expressions;
using Velox.Sql.Core.Impl;

namespace Velox.Sql.Expressions;

internal static class ExpressionParser
{
    private static readonly ConcurrentDictionary<LambdaExpression, MemberUnaryResult[]> MemberUnaryCache = new();
    private static readonly ConcurrentDictionary<LambdaExpression, ExpressionResultEx> ExpressionExCache = new();

    //todo нет саппорта если nullable колонка будет в count(int?)
    public static MemberUnaryResult[] FindMemberUnaryExpression(LambdaExpression exprTree) =>
        MemberUnaryCache.GetOrAdd(exprTree, static e => ParseMemberUnary(e));

    private static MemberUnaryResult[] ParseMemberUnary(LambdaExpression exprTree)
    {
        var result = new List<MemberUnaryResult>();

        switch (exprTree.Body)
        {
            case UnaryExpression expr:
                {
                    var item = (MemberExpression)expr.Operand;
                    result.Add(new MemberUnaryResult { Type = item.Expression.Type, Value = item.Member.Name });
                    break;
                }
            case MemberExpression initExpression:
                result.Add(new MemberUnaryResult
                { Type = initExpression.Expression.Type, Value = initExpression.Member.Name });
                break;
            case NewExpression operation:
                {
                    for (var i = 0; i < operation.Members.Count; i++)
                    {
                        var h = (MemberExpression)operation.Arguments[i];
                        result.Add(new MemberUnaryResult { Type = h.Expression.Type, Value = operation.Members[i].Name });
                    }

                    break;
                }
        }

        return result.Count == 0 ? Array.Empty<MemberUnaryResult>() : result.ToArray();
    }

    public static ExpressionResultEx FindExpressionEx(LambdaExpression exprTree) =>
        ExpressionExCache.GetOrAdd(exprTree, static e => ParseExpressionEx(e));

    private static ExpressionResultEx ParseExpressionEx(LambdaExpression exprTree)
    {
        var orderIndex = 1;
        var item = new ExpressionItemEx();

        if (exprTree.Body.NodeType != ExpressionType.AndAlso && exprTree.Body.NodeType != ExpressionType.OrElse)
            return new ExpressionResultEx
            { Settings = new ExpressionItemEx { LeftExp = ConvertToSqlEx(exprTree.Body) } };

        var operation = (BinaryExpression)exprTree.Body;

        switch (operation.Left)
        {
            case MethodCallExpression method:
                {
                    MethodCallExpression leftExpression = method;
                    if (leftExpression.NodeType == ExpressionType.AndAlso ||
                        leftExpression.NodeType == ExpressionType.OrElse)
                        FindNextExpressionEx(leftExpression, ++orderIndex, item.LeftNext = new ExpressionItemEx());
                    else
                        item.LeftExp = ConvertToSqlEx(leftExpression);
                    break;
                }
            case BinaryExpression binary:
                {
                    BinaryExpression leftExpression = binary;
                    if (leftExpression.NodeType == ExpressionType.AndAlso ||
                        leftExpression.NodeType == ExpressionType.OrElse)
                        FindNextExpressionEx(leftExpression, ++orderIndex, item.LeftNext = new ExpressionItemEx());
                    else
                        item.LeftExp = ConvertToSqlEx(leftExpression);
                    break;
                }
        }

        item.Type = exprTree.Body.NodeType;

        Expression rightExpression = /*(BinaryExpression)*/ operation.Right;
        if (rightExpression.NodeType == ExpressionType.AndAlso ||
            rightExpression.NodeType == ExpressionType.OrElse)
            FindNextExpressionEx(rightExpression, ++orderIndex, item.RightNext = new ExpressionItemEx());
        else
            item.RightExp = ConvertToSqlEx(rightExpression);

        return new ExpressionResultEx { Settings = item };
    }

    private static void FindNextExpressionEx(Expression exp, int orderIndex, ExpressionItemEx item)
    {
        var exprTree = (BinaryExpression)exp;

        if (exprTree.NodeType != ExpressionType.AndAlso && exprTree.NodeType != ExpressionType.OrElse)
        {
            item.LeftExp = ConvertToSqlEx(exprTree);
        }
        else
        {
            switch (exprTree.Left)
            {
                case MethodCallExpression method:
                    {
                        MethodCallExpression leftExpression = method;
                        if (leftExpression.NodeType == ExpressionType.AndAlso ||
                            leftExpression.NodeType == ExpressionType.OrElse)
                            FindNextExpressionEx(leftExpression, ++orderIndex, item.LeftNext = new ExpressionItemEx());
                        else
                            item.LeftExp = ConvertToSqlEx(leftExpression);
                        break;
                    }
                case BinaryExpression binary:
                    {
                        BinaryExpression leftExpression = binary;
                        if (leftExpression.NodeType == ExpressionType.AndAlso ||
                            leftExpression.NodeType == ExpressionType.OrElse)
                            FindNextExpressionEx(leftExpression, ++orderIndex, item.LeftNext = new ExpressionItemEx());
                        else
                            item.LeftExp = ConvertToSqlEx(leftExpression);
                        break;
                    }
            }

            item.Type = exprTree.NodeType;

            switch (exprTree.Right)
            {
                case MethodCallExpression method:
                    {
                        MethodCallExpression rightExpression = method;
                        if (rightExpression.NodeType == ExpressionType.AndAlso ||
                            rightExpression.NodeType == ExpressionType.OrElse)
                            FindNextExpressionEx(rightExpression, ++orderIndex,
                                item.RightNext = new ExpressionItemEx());
                        else
                            item.RightExp = ConvertToSqlEx(rightExpression);
                        break;
                    }
                case BinaryExpression binary:
                    {
                        BinaryExpression rightExpression = binary;
                        if (rightExpression.NodeType == ExpressionType.AndAlso ||
                            rightExpression.NodeType == ExpressionType.OrElse)
                            FindNextExpressionEx(rightExpression, ++orderIndex,
                                item.RightNext = new ExpressionItemEx());
                        else
                            item.RightExp = ConvertToSqlEx(rightExpression);
                        break;
                    }
            }
        }
    }

    private static ExpressionInfoEx ConvertToSqlEx(Expression expr)
    {
        var result = new ExpressionInfoEx();

        if (expr is MethodCallExpression methodCallExpression)
        {
            result.IsFunction = true;
            result.FunctionName = methodCallExpression.Method.Name;
            var fieldName = (MemberExpression)methodCallExpression.Object;

            result.GlobalType = fieldName?.Expression.Type;
            result.LeftOperatorName = fieldName?.Member.Name;
            result.ExpressionType = fieldName?.Type;


            foreach (Expression argument in methodCallExpression.Arguments)
            {
                var convertedArguments = (ConstantExpression)argument;
                result.FunctionParams.Add(new ExpressionFunctionArgumentsEx
                { Type = convertedArguments.Type, Value = convertedArguments.Value });
            }

            return result;
        }


        var expression = (BinaryExpression)expr;
        var @operator = Operators.Equal;

        switch (expression.NodeType)
        {
            case ExpressionType.GreaterThan:
                @operator = Operators.GreaterThan;
                break;
            case ExpressionType.GreaterThanOrEqual:
                @operator = Operators.GreaterThanOrEqual;
                break;
            case ExpressionType.LessThan:
                @operator = Operators.LessThan;
                break;
            case ExpressionType.LessThanOrEqual:
                @operator = Operators.LessThanOrEqual;
                break;
            case ExpressionType.Equal:
                @operator = Operators.Equal;
                break;
            case ExpressionType.NotEqual:
                @operator = Operators.NotEqual;
                break;
        }

        MemberExpression left;

        if (expression.Left.NodeType != ExpressionType.Convert || !IsNullableType(expression.Left.Type))
        {
            if (expression.Left.NodeType == ExpressionType.Convert)
            {
                Expression withoutConvertNodeType = ((UnaryExpression)expression.Left).Operand;
                left = withoutConvertNodeType as MemberExpression;
            }
            else
            {
                left = expression.Left as MemberExpression;
            }
        }
        else
        {
            Expression withoutNullExpression = ((UnaryExpression)expression.Left).Operand;
            left = withoutNullExpression as MemberExpression;
        }

        var prop = expression.Right as MemberExpression;

        //провека для нуллейбл типов
        if (left != null &&
            Nullable.GetUnderlyingType(left.Member.DeclaringType ?? throw new InvalidOperationException()) != null)
            left = left.Expression as MemberExpression;

        if (left == null)
            throw new Exception("Expression type has not been known");

        if (prop != null)
        {
            object value = Expression.Lambda(prop).Compile().DynamicInvoke();

            result.GlobalType = left.Expression.Type;
            result.LeftOperatorName = left.Member.Name;
            result.Operators = @operator;
            result.RightOperatorName = value?.ToString();
            result.RightOperatorValue = value;
            result.ExpressionType = left.Type;

            return result;
        }

        if (expression.Right is UnaryExpression rightUnaryExpression)
        {
            object rValue = Expression.Lambda(rightUnaryExpression).Compile().DynamicInvoke();

            result.GlobalType = left.Expression.Type;
            result.LeftOperatorName = left.Member.Name;
            result.Operators = @operator;
            result.RightOperatorName = rValue.ToString();
            result.RightOperatorValue = rValue;
            result.ExpressionType = rValue.GetType();

            return result;
        }

        var right = (ConstantExpression)expression.Right;

        result.GlobalType = left.Expression.Type;
        result.LeftOperatorName = left.Member.Name;
        result.Operators = @operator;

        if (right.Value == null)
        {
            result.RightOperatorName = "NULL";
            result.RightOperatorValue = null;
            result.ExpressionType = right.Type;
        }
        else
        {
            result.RightOperatorName = right.Value.ToString();
            result.RightOperatorValue = right.Value;
            result.ExpressionType = right.Type;
        }

        return result;
    }

    internal static bool IsNullableType(Type type)
    {
        return type != null && type.IsGenericType && type.GetGenericTypeDefinition() == typeof(Nullable<>);
    }
}