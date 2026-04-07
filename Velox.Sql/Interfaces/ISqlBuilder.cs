using System;
using Velox.Sql.Core.Impl;
using Velox.Sql.Core.Interfaces;

namespace Velox.Sql.Interfaces;

public interface ISqlBuilder : IWhereSubQuery, IDisposable
{
    SqlQuery ToSql();
    string ToDebugSql();
}
