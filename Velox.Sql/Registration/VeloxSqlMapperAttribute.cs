using System;

namespace Velox.Sql.Registration;

/// <summary>
/// Marks an <see cref="Velox.Sql.Impl.Map.IClassMapper"/> implementation so it can be discovered and grouped by engine.
/// Does not change mapper behavior; it is metadata for <see cref="VeloxSqlMapperDiscovery"/>.
/// </summary>
[AttributeUsage(AttributeTargets.Class, Inherited = false, AllowMultiple = false)]
public sealed class VeloxSqlMapperAttribute : Attribute
{
    /// <param name="engines">One or more flags from <see cref="SqlEngine"/>.</param>
    public VeloxSqlMapperAttribute(SqlEngine engines)
    {
        Engines = engines;
    }

    /// <summary>Engines this mapper applies to.</summary>
    public SqlEngine Engines { get; }
}
