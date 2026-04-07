using System;
using System.Linq.Expressions;

namespace Dapper.Extensions.NetCore.Filter
{
    interface IFilter<ClassToFilter>
    {
        Expression<Func<ClassToFilter, bool>> GetFilterPredicateFor(FilterOperations operation, string value);
    }
}
