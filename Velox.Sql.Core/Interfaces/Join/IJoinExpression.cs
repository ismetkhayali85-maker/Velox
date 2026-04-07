namespace Velox.Sql.Core.Interfaces.Join;

public interface IJoinExpression<T> : IJoin<T>
{
    IJoinCondition<T> On(string fromField, ITable fromTable, string toField);
    IJoin<T> Using(string value);
}