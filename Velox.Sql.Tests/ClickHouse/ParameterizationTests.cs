namespace Velox.Sql.Tests.ClickHouse;

public class ParameterizationTests : TestBase
{
    [Fact]
    public void Select_WithParameters_ReturnsCorrectSqlAndDictionary()
    {
        var builder = VeloxRuntime.ClickHouse<TestEntity>();
        builder.Select()
               .Where(x => (x.Id == 10 && x.Name == "Test") || x.Id > 100);

        AssertQuery(builder,
            debug: "SELECT \"test_table\".\"id\" AS \"Id\", \"test_table\".\"name\" AS \"Name\" FROM \"test_table\" WHERE \"test_table\".\"id\" > 100 OR (\"test_table\".\"id\" = 10 AND \"test_table\".\"name\" = 'Test');",
            sql:   "SELECT \"test_table\".\"id\" AS \"Id\", \"test_table\".\"name\" AS \"Name\" FROM \"test_table\" WHERE \"test_table\".\"id\" > @p0 OR (\"test_table\".\"id\" = @p1 AND \"test_table\".\"name\" = @p2);",
            expectedParams: new { p0 = 100, p1 = 10, p2 = "Test" });
    }

    [Fact]
    public void Where_Enum_WithParameters_UsesUnderlyingInteger()
    {
        var builder = VeloxRuntime.ClickHouse<EnumPersistenceEntity>();
        builder.Select()
            .Where(x => x.Kind == PersistenceTestEnum.Beta);

        AssertQuery(builder,
            debug: "SELECT \"ch_kind_row\".\"id\" AS \"Id\", \"ch_kind_row\".\"kind\" AS \"Kind\" FROM \"ch_kind_row\" WHERE \"ch_kind_row\".\"kind\" = 2;",
            sql:   "SELECT \"ch_kind_row\".\"id\" AS \"Id\", \"ch_kind_row\".\"kind\" AS \"Kind\" FROM \"ch_kind_row\" WHERE \"ch_kind_row\".\"kind\" = @p0;",
            expectedParams: new { p0 = 2 });
    }
}
