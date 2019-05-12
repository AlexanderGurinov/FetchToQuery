using System;
using System.Collections.Generic;
using System.Linq;
using System.Xml.Linq;
using SqlKata;

namespace FetchToQuery
{
    internal static class Visitor
    {
        private static readonly IReadOnlyDictionary<string, Action<XElement, Query>> Actions = new Dictionary<string, Action<XElement, Query>>
        {
            { "fetch", VisitFetch },
            { "entity", VisitEntity },
            { "attribute", VisitAttribute },
            { "filter", VisitFilter },
            { "condition", VisitCondition },
            { "link-entity", VisitLink }
        };

        private static readonly Dictionary<string, Func<Query, string, string, Query>> ConditionMap = new Dictionary<string, Func<Query, string, string, Query>>
        {
            { "eq", (query, attribute, value) => query.Where(attribute, "=", value) },
            { "ne", (query, attribute, value) => query.Where(attribute, "!=", value) },
            { "like", (query, attribute, value) => query.Where(attribute, "LIKE", value) },
            { "not-like", (query, attribute, value) => query.Where(attribute, "NOT LIKE", value) },
            { "not-null", (query, attribute, value) => query.Where(attribute, "IS NOT NULL", value) },
            { "null", (query, attribute, value) => query.Where(attribute, "IS NULL", value) },
            { "gt", (query, attribute, value) => query.Where(attribute, ">", value) },
            { "ge", (query, attribute, value) => query.Where(attribute, ">=", value) },
            { "lt", (query, attribute, value) => query.Where(attribute, "<", value) },
            { "le", (query, attribute, value) => query.Where(attribute, "<=", value) },
            { "on", (query, attribute, value) =>
                {
                    var date = DateTime.Parse(value).Date;
                    return query.Where(attribute, ">=", date).Where(attribute, "<", date.AddDays(1));
                }
            },
            { "on-or-after", (query, attribute, value) => query.Where(attribute, ">=", DateTime.Parse(value).Date) },
            { "on-or-before", (query, attribute, value) => query.Where(attribute, "<", DateTime.Parse(value).Date.AddDays(1)) },
            { "yesterday", (query, attribute, value) =>
                {
                    var date = DateTime.UtcNow.Date;
                    return query.Where(attribute, ">=", date.AddDays(-1)).Where(attribute, "<", date);
                }
            },
            { "today", (query, attribute, value) =>
                {
                    var date = DateTime.UtcNow.Date;
                    return query.Where(attribute, ">=", date).Where(attribute, "<", date.AddDays(1));
                }
            },
            { "tomorrow", (query, attribute, value) =>
                {
                    var date = DateTime.UtcNow.Date.AddDays(1);
                    return query.Where(attribute, ">=", date).Where(attribute, "<", date.AddDays(1));
                }
            },
            { "next-seven-days", (query, attribute, value) =>
                {
                    var date = DateTime.UtcNow.Date.AddDays(1);
                    return query.Where(attribute, ">=", date).Where(attribute, "<", date.AddDays(7));
                }
            },
            { "last-seven-days", (query, attribute, value) =>
                {
                    var date = DateTime.UtcNow.Date;
                    return query.Where(attribute, ">=", date.AddDays(-7)).Where(attribute, "<", date);
                }
            },
            { "next-month", (query, attribute, value) =>
                {
                    var date = DateTime.UtcNow;
                    var month = new DateTime(date.Year, date.Month, 1).AddMonths(1);
                    return query.Where(attribute, ">=", month).Where(attribute, "<", month.AddMonths(1));
                }
            },
            { "last-month", (query, attribute, value) =>
                {
                    var date = DateTime.UtcNow;
                    var month = new DateTime(date.Year, date.Month, 1);
                    return query.Where(attribute, ">=", month.AddMonths(-1)).Where(attribute, "<", month);
                }
            },
            { "last-x-months", (query, attribute, value) =>
                {
                    var date = DateTime.UtcNow;
                    var month = new DateTime(date.Year, date.Month, 1);
                    return query.Where(attribute, ">=", month.AddMonths(-int.Parse(value))).Where(attribute, "<", month);
                }
            },
            { "next-x-months", (query, attribute, value) =>
                {
                    var date = DateTime.UtcNow;
                    var month = new DateTime(date.Year, date.Month, 1).AddMonths(1);
                    return query.Where(attribute, ">=", month).Where(attribute, "<", month.AddMonths(int.Parse(value)));
                }
            },
            { "last-x-years", (query, attribute, value) =>
                {
                    var date = DateTime.UtcNow;
                    var year = new DateTime(date.Year, 1, 1);
                    return query.Where(attribute, ">=", year.AddYears(-int.Parse(value))).Where(attribute, "<", year);
                }
            },
            { "next-x-years", (query, attribute, value) =>
                {
                    var year = new DateTime(DateTime.UtcNow.Year, 1, 1).AddYears(1);
                    return query.Where(attribute, ">=", year).Where(attribute, "<", year.AddYears(int.Parse(value)));
                }
            },
            { "this-month", (query, attribute, value) =>
                {
                    var date = DateTime.UtcNow;
                    var month = new DateTime(date.Year, date.Month, 1);
                    return query.Where(attribute, ">=", month).Where(attribute, "<", month.AddMonths(1));
                }
            },
            { "next-year", (query, attribute, value) =>
                {
                    var year = new DateTime(DateTime.UtcNow.Year, 1, 1).AddYears(1);
                    return query.Where(attribute, ">=", year).Where(attribute, "<", year.AddYears(1));
                }
            },
            { "last-year", (query, attribute, value) =>
                {
                    var year = new DateTime(DateTime.UtcNow.Year, 1, 1);
                    return query.Where(attribute, ">=", year.AddYears(-1)).Where(attribute, "<", year);
                }
            },
            { "this-year", (query, attribute, value) =>
                {
                    var year = new DateTime(DateTime.UtcNow.Year, 1, 1);
                    return query.Where(attribute, ">=", year).Where(attribute, "<", year.AddYears(1));
                }
            },
            { "last-x-days", (query, attribute, value) =>
                {
                    var date = DateTime.UtcNow.Date;
                    return query.Where(attribute, ">=", date.AddDays(-int.Parse(value))).Where(attribute, "<", date);
                }
            },
            { "next-x-days", (query, attribute, value) =>
                {
                    var date = DateTime.UtcNow.Date.AddDays(1);
                    return query.Where(attribute, ">=", date).Where(attribute, "<", date.AddDays(int.Parse(value)));
                }
            },
            { "olderthan-x-days", (query, attribute, value) => query.Where(attribute, "<", DateTime.UtcNow.Date.AddDays(-int.Parse(value))) },
            { "olderthan-x-months", (query, attribute, value) => query.Where(attribute, "<", DateTime.UtcNow.Date.AddMonths(-int.Parse(value))) },
            { "olderthan-x-years", (query, attribute, value) => query.Where(attribute, "<", DateTime.UtcNow.Date.AddYears(-int.Parse(value))) }
        };

        private static void VisitLink(XElement element, Query query)
        {
            var attributes = GetAttributes(element);

            var name = attributes["name"];
            var from = attributes["from"];
            var to = attributes["to"];
            var linkType = attributes.TryGetValue("link-type", out var link) ? link.ToLower() : "inner";

            var join = query.When(linkType == "inner",
                    q => q.Join(name, from, to),
                    q => q.LeftJoin(name, from, to));

            if (attributes.TryGetValue("alias", out var alias))
            {
                join.As(alias);
            }
        }

        private static void VisitCondition(XElement element, Query query)
        {
            var attributes = GetAttributes(element);

            var attribute = attributes["attribute"];
            var @operator = attributes["operator"].ToLower();
            var value = attributes.TryGetValue("value", out var val) ? val : default;

            if (ConditionMap.TryGetValue(@operator, out var condition))
            {
                condition(query, attribute, value);
            }
            else
            {
                throw new NotImplementedException($"Unknown operator '{@operator}'");
            }
        }

        public static void Visit(XElement element, Query query)
        {
            var action = Actions[element.Name.LocalName.ToLower()];
            action(element, query);
        }

        private static void Visit(IEnumerable<XElement> elements, Query query)
        {
            foreach (var element in elements)
            {
                Visit(element, query);
            }
        }

        private static IReadOnlyDictionary<string, string> GetAttributes(XElement element) => element.Attributes()
            .ToDictionary(i => i.Name.LocalName.ToLower(), i => i.Value);

        private static void VisitFetch(XElement element, Query query)
        {
            var attributes = GetAttributes(element);

            if (attributes.TryGetValue("distinct", out var value) && bool.TryParse(value, out var boolValue) && boolValue)
            {
                query.Distinct();
            }

            if (attributes.TryGetValue("top", out value))
            {
                query.Take(int.Parse(value));
            }

            Visit(element.Elements(), query);
        }

        private static void VisitEntity(XElement element, Query query)
        {
            var attributes = GetAttributes(element);

            var name = attributes["name"];
            var from = query.From(name);

            if (attributes.TryGetValue("alias", out var alias))
            {
                from.As(alias);
            }

            Visit(element.Elements(), query);
        }

        private static void VisitAttribute(XElement element, Query query)
        {
            var attributes = GetAttributes(element);

            var name = attributes["name"];
            var select = query.Select(name);

            if (attributes.TryGetValue("alias", out var alias))
            {
                select.As(alias);
            }
        }

        private static void VisitFilter(XElement element, Query query)
        {
            Query Callback(Query q)
            {
                Visit(element.Elements(), q);
                return q;
            }

            var attributes = GetAttributes(element);

            query.When(attributes.TryGetValue("type", out var value) && value == "or",
                q => q.OrWhere(Callback),
                q => q.Where(Callback));
        }
    }
}
