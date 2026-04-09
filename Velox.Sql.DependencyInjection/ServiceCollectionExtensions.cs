using System.Reflection;
using Microsoft.Extensions.DependencyInjection;
using Velox.Sql.Impl;
using Velox.Sql.Registration;

namespace Velox.Sql.DependencyInjection;

/// <summary>Registers PostgreSQL and ClickHouse mapper configurations using <see cref="VeloxSqlMapperDiscovery"/>.</summary>
public static class ServiceCollectionExtensions
{
    /// <summary>
    /// Scans <paramref name="mapperAssemblies"/> for attributed mappers and registers
    /// <see cref="VeloxSqlDiscoveryResult"/>, <see cref="PgSqlConfiguration"/>, and <see cref="ClickHouseSqlConfiguration"/> as singletons.
    /// </summary>
    public static IServiceCollection AddVeloxSql(this IServiceCollection services, params Assembly[] mapperAssemblies) =>
        services.AddVeloxSql(configure: null, mapperAssemblies);

    /// <param name="configure">Optional callback to add or adjust <see cref="VeloxSqlDependencyInjectionOptions.MapperAssemblies"/>.</param>
    public static IServiceCollection AddVeloxSql(
        this IServiceCollection services,
        Action<VeloxSqlDependencyInjectionOptions>? configure,
        params Assembly[] mapperAssemblies)
    {
        ArgumentNullException.ThrowIfNull(services);

        var options = new VeloxSqlDependencyInjectionOptions();
        foreach (var a in mapperAssemblies)
            options.MapperAssemblies.Add(a);
        configure?.Invoke(options);

        if (options.MapperAssemblies.Count == 0)
            throw new ArgumentException("Provide at least one assembly containing VeloxSqlMapperAttribute mappers.", nameof(mapperAssemblies));

        services.AddSingleton(_ => VeloxSqlMapperDiscovery.Discover(options.MapperAssemblies));
        services.AddSingleton(sp => sp.GetRequiredService<VeloxSqlDiscoveryResult>().CreatePostgresConfiguration());
        services.AddSingleton(sp => sp.GetRequiredService<VeloxSqlDiscoveryResult>().CreateClickHouseConfiguration());

        return services;
    }
}
