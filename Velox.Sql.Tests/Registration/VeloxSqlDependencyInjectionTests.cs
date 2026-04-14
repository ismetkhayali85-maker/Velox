using Microsoft.Extensions.DependencyInjection;
using Velox.Sql.DependencyInjection;
using Velox.Sql.Impl;
using Velox.Sql.Registration;
using Velox.Sql.Tests.Support;

namespace Velox.Sql.Tests.Registration;

/// <summary>
/// DI registration: resolve <see cref="PgSqlConfiguration"/> / <see cref="ClickHouseSqlConfiguration"/> from the container.
/// </summary>
public sealed class VeloxSqlDependencyInjectionTests
{
    [Fact]
    public void AddVeloxSql_RegistersConfigurations_AsSingletons()
    {
        var services = new ServiceCollection();
        services.AddVeloxSql(typeof(DiscoveryPgEntityMapper).Assembly);

        using var provider = services.BuildServiceProvider();

        var pg = provider.GetRequiredService<PgSqlConfiguration>();
        var ch = provider.GetRequiredService<ClickHouseSqlConfiguration>();
        var discovery = provider.GetRequiredService<VeloxSqlDiscoveryResult>();

        Assert.Same(pg, provider.GetRequiredService<PgSqlConfiguration>());
        Assert.Same(ch, provider.GetRequiredService<ClickHouseSqlConfiguration>());

        Assert.Equal(typeof(DiscoveryPgEntity), pg.GetMap(typeof(DiscoveryPgEntity)).EntityType);
        Assert.Equal(typeof(DiscoveryChEntity), ch.GetMap(typeof(DiscoveryChEntity)).EntityType);
        Assert.NotEmpty(discovery.PostgresMappers);
        Assert.NotEmpty(discovery.ClickHouseMappers);

        var velox = provider.GetRequiredService<IVeloxSql>();
        Assert.Same(pg, provider.GetRequiredService<PgSqlConfiguration>());
        Assert.Same(ch, provider.GetRequiredService<ClickHouseSqlConfiguration>());
        Assert.NotNull(velox.Postgres<DiscoveryPgEntity>());
        Assert.NotNull(velox.ClickHouse<DiscoveryChEntity>());
    }

    [Fact]
    public void AddVeloxSql_WithConfigureCallback_CanAdjustAssembliesList()
    {
        var services = new ServiceCollection();
        services.AddVeloxSql(
            o =>
            {
                // Example: same assembly already added via params; callback can append more if needed.
                Assert.NotEmpty(o.MapperAssemblies);
            },
            typeof(DiscoveryPgEntityMapper).Assembly);

        using var provider = services.BuildServiceProvider();
        Assert.NotNull(provider.GetService<PgSqlConfiguration>());
    }

    [Fact]
    public void AddVeloxSql_WithNoAssemblies_Throws()
    {
        var services = new ServiceCollection();
        Assert.Throws<ArgumentException>(() => services.AddVeloxSql());
    }
}
