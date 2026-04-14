using BenchmarkDotNet.Attributes;
using Velox.Sql.Impl;
using Velox.Sql.Impl.Map;

namespace Velox.Sql.Test.Benchmarks;

[MemoryDiagnoser]
public class SqlBuilderBenchmarks
{
    public class BenchmarkEntity
    {
        public int Id { get; set; }
        public string Name { get; set; }
        public string Description { get; set; }
        public DateTime CreatedAt { get; set; }
    }

    public class BenchmarkEntityMapper : Mapper<BenchmarkEntity>
    {
        public BenchmarkEntityMapper()
        {
            Table("benchmark_table");
            Map(x => x.Id).Column("id");
            Map(x => x.Name).Column("name");
            Map(x => x.Description).Column("description");
            Map(x => x.CreatedAt).Column("created_at");
            Build();
        }
    }

    private IVeloxSql _sql = null!;

    [GlobalSetup]
    public void Setup()
    {
        var mappers = new List<IClassMapper> { new BenchmarkEntityMapper() };
        var pg = new PgSqlConfiguration(mappers);
        var ch = new ClickHouseSqlConfiguration(mappers);
        _sql = new VeloxSql(pg, ch);
    }

    [Benchmark]
    public string ClickHouse_SimpleSelect()
    {
        return _sql.ClickHouse<BenchmarkEntity>()
            .ToDebugSql();
    }

    [Benchmark]
    public string Postgres_SimpleSelect()
    {
        return _sql.Postgres<BenchmarkEntity>()
            .ToDebugSql();
    }

    [Benchmark]
    public string ClickHouse_ComplexQuery()
    {
        return _sql.ClickHouse<BenchmarkEntity>()
            .Select(x => x.Id)
            .Where(x => x.Id > 100)
            .GroupBy(x => x.Id)
            .Having(h => h.Count(x => x.Id, Velox.Sql.Core.Impl.Operators.GreaterThan, 5))
            .OrderByAsc(x => x.Id)
            .Limit(10)
            .ToDebugSql();
    }

    [Benchmark]
    public string Postgres_ComplexQuery()
    {
        return _sql.Postgres<BenchmarkEntity>()
            .Select(x => x.Id)
            .Where(x => x.Id > 100)
            .GroupBy(x => x.Id)
            .Having(h => h.Count(x => x.Id, Velox.Sql.Core.Impl.Operators.GreaterThan, 5))
            .OrderByAsc(x => x.Id)
            .Limit(10)
            .ToDebugSql();
    }
}
