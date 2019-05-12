using SqlKata.Compilers;
using Xunit;

namespace FetchToQuery.Tests
{
    public sealed class FetchToQueryTest
    {
        [Theory, InlineData(
@"<fetch top=""1"" distinct=""true"" >
  <entity name=""contact"" >
    <attribute name=""lastname"" />
    <attribute name=""telephone1"" />
    <attribute name=""firstname"" />
    <filter>
      <condition attribute=""telephone1"" operator=""eq"" value=""380000000001"" />
    </filter>
    <link-entity name=""account"" from=""accountid"" to=""sb_districtid"" />
  </entity>
</fetch>")]
        public void Parse_FetchXml_ShouldSucceed(string fetchXml)
        {
            // Arrange
            var compiler = new SqlServerCompiler();

            // Act
            var query = FetchToQuery.Parse(fetchXml);
            var result = compiler.Compile(query);

            // Assert
            Assert.NotNull(result.ToString());
        }
    }
}
