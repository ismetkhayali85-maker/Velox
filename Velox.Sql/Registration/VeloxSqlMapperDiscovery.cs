using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using Velox.Sql.Impl.Map;

namespace Velox.Sql.Registration;

/// <summary>
/// Discovers <see cref="IClassMapper"/> types marked with <see cref="VeloxSqlMapperAttribute"/> in the given assemblies or type list.
/// </summary>
public static class VeloxSqlMapperDiscovery
{
    /// <summary>
    /// Scans <paramref name="assemblies"/> for concrete <see cref="IClassMapper"/> types with <see cref="VeloxSqlMapperAttribute"/>,
    /// instantiates them (parameterless constructor), and groups instances by engine.
    /// </summary>
    /// <exception cref="InvalidOperationException">Duplicate <see cref="IClassMapper.EntityType"/> for the same engine, or invalid attribute/engine.</exception>
    public static VeloxSqlDiscoveryResult Discover(params Assembly[] assemblies) =>
        Discover((IEnumerable<Assembly>)assemblies);

    /// <inheritdoc cref="Discover(Assembly[])"/>
    public static VeloxSqlDiscoveryResult Discover(IEnumerable<Assembly> assemblies)
    {
        ArgumentNullException.ThrowIfNull(assemblies);

        var postgres = new List<IClassMapper>();
        var clickHouse = new List<IClassMapper>();

        foreach (var assembly in assemblies)
        {
            if (assembly == null)
                throw new ArgumentException("Assemblies collection contains a null entry.", nameof(assemblies));

            foreach (var type in GetLoadableTypes(assembly))
                TryProcessMapperType(type, postgres, clickHouse);
        }

        return new VeloxSqlDiscoveryResult(postgres, clickHouse);
    }

    /// <summary>
    /// Processes only the given mapper <paramref name="mapperTypes"/> (no assembly scan). Types without
    /// <see cref="VeloxSqlMapperAttribute"/> are skipped, same as when scanning assemblies.
    /// </summary>
    public static VeloxSqlDiscoveryResult DiscoverTypes(params Type[] mapperTypes) =>
        DiscoverTypes((IEnumerable<Type>)mapperTypes);

    /// <inheritdoc cref="DiscoverTypes(Type[])"/>
    public static VeloxSqlDiscoveryResult DiscoverTypes(IEnumerable<Type> mapperTypes)
    {
        ArgumentNullException.ThrowIfNull(mapperTypes);

        var postgres = new List<IClassMapper>();
        var clickHouse = new List<IClassMapper>();

        foreach (var type in mapperTypes)
        {
            if (type == null)
                throw new ArgumentException("Mapper types collection contains a null entry.", nameof(mapperTypes));

            TryProcessMapperType(type, postgres, clickHouse);
        }

        return new VeloxSqlDiscoveryResult(postgres, clickHouse);
    }

    static void TryProcessMapperType(Type type, List<IClassMapper> postgres, List<IClassMapper> clickHouse)
    {
        if (type is not { IsClass: true, IsAbstract: false })
            return;

        if (!typeof(IClassMapper).IsAssignableFrom(type))
            return;

        var attr = type.GetCustomAttribute<VeloxSqlMapperAttribute>(inherit: false);
        if (attr == null)
            return;

        if (attr.Engines == SqlEngine.None)
            throw new InvalidOperationException($"Mapper '{type.FullName}' specifies {nameof(SqlEngine)}.{nameof(SqlEngine.None)}.");

        var instance = CreateMapperInstance(type);

        if ((attr.Engines & SqlEngine.PostgreSQL) != 0)
            AddWithDuplicateCheck(postgres, instance, SqlEngine.PostgreSQL, type);

        if ((attr.Engines & SqlEngine.ClickHouse) != 0)
            AddWithDuplicateCheck(clickHouse, instance, SqlEngine.ClickHouse, type);
    }

    static IClassMapper CreateMapperInstance(Type type)
    {
        try
        {
            // Supports public and non-public parameterless constructors (typical for nested mapper types).
            var instance = Activator.CreateInstance(type, nonPublic: true);
            if (instance is IClassMapper mapper)
                return mapper;
            throw new InvalidOperationException($"Type '{type.FullName}' is not an {nameof(IClassMapper)}.");
        }
        catch (Exception ex) when (ex is not InvalidOperationException)
        {
            throw new InvalidOperationException($"Failed to create mapper instance for '{type.FullName}'. Ensure a parameterless constructor.", ex);
        }
    }

    static void AddWithDuplicateCheck(List<IClassMapper> list, IClassMapper mapper, SqlEngine engine, Type mapperType)
    {
        var entityType = mapper.EntityType;
        if (entityType is null)
            return;

        for (var i = 0; i < list.Count; i++)
        {
            var existing = list[i];
            if (existing.EntityType == entityType)
            {
                throw new InvalidOperationException(
                    $"Duplicate mapper for entity '{entityType.FullName}' on {engine}: '{mapperType.FullName}' and '{existing.GetType().FullName}'.");
            }
        }

        list.Add(mapper);
    }

    static IEnumerable<Type> GetLoadableTypes(Assembly assembly)
    {
        try
        {
            return assembly.GetTypes();
        }
        catch (ReflectionTypeLoadException ex)
        {
            return ex.Types.Where(t => t != null).Cast<Type>();
        }
    }
}
