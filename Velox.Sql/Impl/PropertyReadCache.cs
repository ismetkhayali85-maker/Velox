using System;
using System.Collections.Concurrent;
using System.Reflection;

namespace Velox.Sql.Impl;

/// <summary>
/// Caches <see cref="PropertyInfo"/> lookups for entity mapping (insert/update/bulk).
/// </summary>
internal static class PropertyReadCache
{
    private static readonly ConcurrentDictionary<(Type DeclaringType, string Name), PropertyInfo> Cache = new();

    public static (Type PropertyType, object Value) GetPropValue(object src, string propName)
    {
        var type = src.GetType();
        var prop = Cache.GetOrAdd((type, propName), static key => key.DeclaringType.GetProperty(key.Name));
        return (prop.PropertyType, prop.GetValue(src, null));
    }
}
