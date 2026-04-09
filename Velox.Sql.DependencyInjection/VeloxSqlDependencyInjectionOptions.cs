using System.Reflection;

namespace Velox.Sql.DependencyInjection;

/// <summary>Options for <see cref="ServiceCollectionExtensions.AddVeloxSql"/>.</summary>
public sealed class VeloxSqlDependencyInjectionOptions
{
    /// <summary>
    /// Assemblies to scan for <see cref="Velox.Sql.Registration.VeloxSqlMapperAttribute"/> mappers.
    /// Filled from parameters to <c>AddVeloxSql</c> before the optional configure delegate runs.
    /// </summary>
    public IList<Assembly> MapperAssemblies { get; } = new List<Assembly>();
}
