using System.Reflection;
using System.Resources;
using System.Xml;
using System.Xml.Linq;
using System.Xml.Schema;
using SqlKata;

namespace FetchToQuery
{
    public static class FetchToQuery
    {
        public static Query Parse(string fetchXml)
        {
            var document = XDocument.Parse(fetchXml);
            Validate(document);
            var root = document.Root;
            var query = new Query();
            Visitor.Visit(root, query);
            return query;
        }

        private static void Validate(XDocument document)
        {
            var assembly = Assembly.GetExecutingAssembly();
            using (var stream = assembly.GetManifestResourceStream($"{nameof(FetchToQuery)}.Fetch.xsd"))
            using (var reader = XmlReader.Create(stream ?? throw new MissingManifestResourceException()))
            {
                var schemas = new XmlSchemaSet();
                schemas.Add(string.Empty, reader);
                document.Validate(schemas, null);
            }
        }
    }
}
