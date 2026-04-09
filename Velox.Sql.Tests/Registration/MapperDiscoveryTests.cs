using System.Reflection;
using Velox.Sql.Impl;
using Velox.Sql.Registration;
using Velox.Sql.Tests.Support;

namespace Velox.Sql.Tests.Registration;

/// <summary>
/// Documents configuration styles: (1) <see cref="VeloxSqlMapperDiscovery.Discover(System.Reflection.Assembly[])"/>,
/// (2) <see cref="VeloxSqlDiscoveryResult.ApplyToDbQuery"/> for static <see cref="DbQuery"/> defaults,
/// (3) DI in <see cref="VeloxSqlDependencyInjectionTests"/>.
/// Use <see cref="Velox.Sql.Tests.Support"/> assembly for clean assembly scans (no duplicate-entity conflicts).
/// </summary>
public sealed class MapperDiscoveryTests
{
    static readonly Assembly SupportAssembly = typeof(DiscoveryPgEntityMapper).Assembly;

    [Fact]
    public void Discover_SplitsMappers_BySqlEngine()
    {
        var result = VeloxSqlMapperDiscovery.Discover(SupportAssembly);

        Assert.Equal(2, result.PostgresMappers.Count);
        Assert.Equal(2, result.ClickHouseMappers.Count);
        Assert.Contains(result.PostgresMappers, m => m.EntityType == typeof(DiscoveryPgEntity));
        Assert.Contains(result.PostgresMappers, m => m.EntityType == typeof(DiscoverySharedEntity));
        Assert.Contains(result.ClickHouseMappers, m => m.EntityType == typeof(DiscoveryChEntity));
        Assert.Contains(result.ClickHouseMappers, m => m.EntityType == typeof(DiscoverySharedEntity));
    }

    [Fact]
    public void Discover_CombinedFlags_RegistersSameMapperTypeForBothEngines()
    {
        var result = VeloxSqlMapperDiscovery.Discover(SupportAssembly);

        Assert.Contains(result.PostgresMappers, m => m.EntityType == typeof(DiscoverySharedEntity));
        Assert.Contains(result.ClickHouseMappers, m => m.EntityType == typeof(DiscoverySharedEntity));
        Assert.IsType<DiscoverySharedEntityMapper>(result.PostgresMappers.Single(m => m.EntityType == typeof(DiscoverySharedEntity)));
        Assert.IsType<DiscoverySharedEntityMapper>(result.ClickHouseMappers.Single(m => m.EntityType == typeof(DiscoverySharedEntity)));
    }

    [Fact]
    public void Discover_IgnoresMapperClassesWithoutAttribute()
    {
        var result = VeloxSqlMapperDiscovery.DiscoverTypes(
            typeof(DiscoveryUnmarkedEntityMapper),
            typeof(DiscoveryPgEntityMapper));

        Assert.DoesNotContain(result.PostgresMappers, m => m.EntityType == typeof(DiscoveryUnmarkedEntity));
        Assert.Single(result.PostgresMappers);
    }

    [Fact]
    public void DiscoverTypes_ThrowsWhenSameEntityMappedTwice_ForSameEngine()
    {
        var ex = Assert.Throws<InvalidOperationException>(() =>
            VeloxSqlMapperDiscovery.DiscoverTypes(typeof(DiscoveryConflictMapperA), typeof(DiscoveryConflictMapperB)));

        Assert.Contains("Duplicate mapper for entity", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void CreateConfigurations_BuildsWorkingPgAndClickHouseConfigs()
    {
        var result = VeloxSqlMapperDiscovery.Discover(SupportAssembly);
        var pg = result.CreatePostgresConfiguration();
        var ch = result.CreateClickHouseConfiguration();

        var pgMap = pg.GetMap(typeof(DiscoveryPgEntity));
        Assert.Equal("discovery_pg", pgMap.TableName);

        var chMap = ch.GetMap(typeof(DiscoveryChEntity));
        Assert.Equal("discovery_ch", chMap.TableName);
    }

    [Fact]
    public void ApplyToDbQuery_SetsStaticDefaults_LikeManualStaticConfiguration()
    {
        var previousPg = DbQuery.DefaultPostgresConfig;
        var previousCh = DbQuery.DefaultClickHouseConfig;
        try
        {
            var result = VeloxSqlMapperDiscovery.Discover(SupportAssembly);
            result.ApplyToDbQuery();

            Assert.NotNull(DbQuery.DefaultPostgresConfig);
            Assert.NotNull(DbQuery.DefaultClickHouseConfig);

            var sql = DbQuery<DiscoveryPgEntity>.GetPostgresBuilder()
                .Select(x => x.Id)
                .ToDebugSql();

            Assert.Contains("discovery_pg", sql, StringComparison.Ordinal);
        }
        finally
        {
            DbQuery.DefaultPostgresConfig = previousPg;
            DbQuery.DefaultClickHouseConfig = previousCh;
        }
    }
}
