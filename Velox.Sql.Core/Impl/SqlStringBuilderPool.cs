using System;
using System.Text;

namespace Velox.Sql.Core.Impl;

/// <summary>
/// Reuses one <see cref="StringBuilder"/> per thread for short-lived SQL fragments (e.g. column names).
/// </summary>
internal static class SqlStringBuilderPool
{
    [ThreadStatic]
    private static StringBuilder t_cached;

    public static StringBuilder Rent(int capacity = 64)
    {
        var sb = t_cached;
        if (sb != null)
        {
            t_cached = null;
            sb.Clear();
            if (sb.Capacity < capacity)
                sb.Capacity = capacity;
            return sb;
        }

        return capacity > 64 ? new StringBuilder(capacity) : new StringBuilder(64);
    }

    public static void Return(StringBuilder sb)
    {
        if (sb == null || sb.Capacity > 65_536)
            return;
        t_cached = sb;
    }
}
