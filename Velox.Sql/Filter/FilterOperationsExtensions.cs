using System;
using System.Linq.Expressions;

namespace Dapper.Extensions.NetCore.Filter
{
    public enum FilterOperations
    {
        None,
        Equal,
        NotEqual,
        LessThan,
        LessThanOrEqual,
        GreaterThan,
        GreaterThanOrEqual,
        Contains
    }

    public static class FilterOperationsExtensions
    {
        public static ExpressionType ToExpressionType(this FilterOperations operand)
        {
            switch (operand)
            {
                case FilterOperations.Equal: return ExpressionType.Equal;

                case FilterOperations.NotEqual: return ExpressionType.NotEqual;

                case FilterOperations.LessThan: return ExpressionType.LessThan;

                case FilterOperations.LessThanOrEqual: return ExpressionType.LessThanOrEqual;

                case FilterOperations.GreaterThan: return ExpressionType.GreaterThan;

                case FilterOperations.GreaterThanOrEqual: return ExpressionType.GreaterThanOrEqual;

                default: throw new NotSupportedException();

            }
        }
    }
}