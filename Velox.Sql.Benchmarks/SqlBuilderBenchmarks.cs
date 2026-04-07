using BenchmarkDotNet.Attributes;
using Velox.Sql;
using Velox.Sql.Impl;
using Velox.Sql.Impl.Map;
using System.Collections.Generic;

namespace Velox.Sql.Benchmarks;

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

    [GlobalSetup]
    public void Setup()
    {
        var mappers = new List<IClassMapper> { new BenchmarkEntityMapper() };
        DbQuery.DefaultClickHouseConfig = new ClickHouseSqlConfiguration(mappers);
        DbQuery.DefaultPostgresConfig = new PgSqlConfiguration(mappers);
    }

    [Benchmark]
    public string ClickHouse_SimpleSelect()
    {
        return DbQuery<BenchmarkEntity>.GetClickHouseBuilder()
            .ToDebugSql();
    }

    [Benchmark]
    public string Postgres_SimpleSelect()
    {
        return DbQuery<BenchmarkEntity>.GetPostgresBuilder()
            .ToDebugSql();
    }

    [Benchmark]
    public string ClickHouse_ComplexQuery()
    {
        return DbQuery<BenchmarkEntity>.GetClickHouseBuilder()
            .Select(x => x.Id)
            .Where(x => x.Id > 100)
            .GroupBy(x => x.Id)
            .Having(h => h.Count(x => x.Id, Velox.Sql.Core.Impl.Operators.GreaterThan, 5))
            .OrderBy(true, x => x.Id)
            .Limit(10)
            .ToDebugSql();
    }

    [Benchmark]
    public string Postgres_ComplexQuery()
    {
        return DbQuery<BenchmarkEntity>.GetPostgresBuilder()
            .Select(x => x.Id)
            .Where(x => x.Id > 100)
            .GroupBy(x => x.Id)
            .Having(h => h.Count(x => x.Id, Velox.Sql.Core.Impl.Operators.GreaterThan, 5))
            .OrderBy(true, x => x.Id)
            .Limit(10)
            .ToDebugSql();
    }
}
