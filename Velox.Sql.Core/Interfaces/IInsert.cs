using System.Collections.Generic;

namespace Velox.Sql.Core.Interfaces;

public interface IInsert
{
    IInsert InsertValuePairs(Dictionary<string, IValue> valuePairs);
    IInsert Expression<TBuilder>(ISqlBuilder<TBuilder> builder);
    IInsert BulkInsertValuePairs(List<Dictionary<string, IValue>> valuePairs);
}