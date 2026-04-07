namespace Velox.Sql.Tests.ClickHouse;

public class ParameterizationTests : TestBase
{
    [Fact]
    public void Select_WithParameters_ReturnsCorrectSqlAndDictionary()
    {
        var builder = DbQuery<TestEntity>.GetClickHouseBuilder();
        builder.Select()
               .Where(x => (x.Id == 10 && x.Name == "Test") || x.Id > 100);

        AssertQuery(builder,
            debug: "SELECT \"test_table\".\"id\" AS \"Id\", \"test_table\".\"name\" AS \"Name\" FROM \"test_table\" WHERE \"test_table\".\"id\" > 100 OR (\"test_table\".\"id\" = 10 AND \"test_table\".\"name\" = 'Test');",
            sql:   "SELECT \"test_table\".\"id\" AS \"Id\", \"test_table\".\"name\" AS \"Name\" FROM \"test_table\" WHERE \"test_table\".\"id\" > @p0 OR (\"test_table\".\"id\" = @p1 AND \"test_table\".\"name\" = @p2);",
            expectedParams: new { p0 = 100, p1 = 10, p2 = "Test" });
    }
}
