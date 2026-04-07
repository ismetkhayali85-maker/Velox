using Velox.Sql.Core.PostgreSql;

namespace Velox.Sql.Core.Impl;

public class SqlFactory
{
    public PostgreSqlBuilder GetPostgreSqlBuilder()
    {
        return new PostgreSqlBuilder();
    }
}