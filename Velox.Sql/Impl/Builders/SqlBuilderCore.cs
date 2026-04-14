using System.Collections.Generic;
using Velox.Sql.Core.Impl;
using Velox.Sql.Interfaces;

namespace Velox.Sql.Impl.Builders;

/// <summary>
/// Core base class for all SQL builders, providing shared state for parameters and query generation.
/// </summary>
public abstract class SqlBuilderCore<TEntity> : ISqlBuilder
{
    protected Dictionary<string, object> _currentParameters;
    protected int _paramCounter;

    public abstract SqlQuery ToSql();
    public abstract string ToDebugSql();
    public abstract string GetSql();
    public abstract void Dispose();
}
