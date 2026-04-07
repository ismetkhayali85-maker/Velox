using System.Collections.Generic;

namespace Velox.Sql.Core.Interfaces;

public interface IReturning
{
    string SetValue(IColumn returnItem);
    string SetValue(List<IColumn> returnItems);
    string All();
}