namespace Velox.Sql.Core.Interfaces.Join;

public interface IJoinCondition<T> : IJoin<T>
{
    IJoinCondition<T> And(string tableField, IValue value);
}