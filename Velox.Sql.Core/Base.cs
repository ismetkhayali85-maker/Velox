using System.Text;

namespace Velox.Sql.Core;

public class Base
{
    internal StringBuilder Sql = new StringBuilder();

    public override string ToString()
    {
        return Sql.ToString();
    }
}