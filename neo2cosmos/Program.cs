using System;
using System.Collections.Generic;
using System.Configuration;
using System.Threading.Tasks;
using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;
using Microsoft.Azure.Graphs;
using Neo4j.Driver.V1;
using System.Diagnostics;
using System.Linq;
using System.Net;

namespace neo2cosmos
{
    public class Program
    {
        private const string DatabaseId = "graphdb";
        private const string CollectionId = "Northwind";

        private static string _cosmosEndpoint;
        private static string _cosmosAuthKey;

        private static string _neoBolt;
        private static string _neoUser;
        private static string _neoPass;

        public static void Main(string[] args)
        {
            LoadConfiguration();

            using (var client = new DocumentClient(
                new Uri(_cosmosEndpoint),
                _cosmosAuthKey,
                new ConnectionPolicy {ConnectionMode = ConnectionMode.Direct, ConnectionProtocol = Protocol.Tcp}))
            {
                var p = new Program();
                p.RunAsync(client).Wait();
            }
        }

        private static void LoadConfiguration()
        {
            _cosmosEndpoint = ConfigurationManager.AppSettings["CosmosEndpoint"];
            _cosmosAuthKey = ConfigurationManager.AppSettings["CosmosAuthKey"];

            _neoBolt = ConfigurationManager.AppSettings["NeoBolt"];
            _neoUser = ConfigurationManager.AppSettings["NeoUser"];
            _neoPass = ConfigurationManager.AppSettings["NeoPass"];
        }

        public async Task RunAsync(DocumentClient client)
        {
            await client.CreateDatabaseIfNotExistsAsync(new Database {Id = DatabaseId});

            try
            {
                await client.DeleteDocumentCollectionAsync(
                    UriFactory.CreateDocumentCollectionUri(DatabaseId, CollectionId));
            }
            catch (DocumentClientException e)
            {
                if (e.StatusCode != HttpStatusCode.NotFound)
                    throw;
            }

            var graph = await client.CreateDocumentCollectionIfNotExistsAsync(
                UriFactory.CreateDatabaseUri(DatabaseId),
                new DocumentCollection {Id = CollectionId},
                new RequestOptions {OfferThroughput = 400});

            await CreateVertexes(client, graph);
            await CreateEdges(client, graph);
        }

        private static async Task CreateVertexes(DocumentClient client, DocumentCollection graph)
        {
            var s = new Stopwatch();
            s.Start();
            await Task.Run(() =>
                Parallel.ForEach(ReadNeoVertexes(), new ParallelOptions {MaxDegreeOfParallelism = 4}, item =>
                {
                    var gremlinVertex = ConvertItemToGremlinVertex(item);
                    Console.WriteLine(gremlinVertex);
                    var query = client.CreateGremlinQuery<dynamic>(graph, gremlinVertex);
                    while (query.HasMoreResults)
                    {
                        var sw = new Stopwatch();
                        sw.Start();
                        query.ExecuteNextAsync().Wait();
                        Console.WriteLine($"Vertex created in: {s.ElapsedMilliseconds} ms");
                    }
                    Console.WriteLine();
                })
            );
            Console.WriteLine($"Total Time: {s.ElapsedMilliseconds} ms");
        }

        private static async Task CreateEdges(DocumentClient client, DocumentCollection graph)
        {
            var s = new Stopwatch();
            s.Start();
            await Task.Run(() =>
                Parallel.ForEach(ReadNeoEdges(), new ParallelOptions {MaxDegreeOfParallelism = 4}, item =>
                {
                    var gremlinEdge = ConvertItemToGremlinEdge(item);
                    Console.WriteLine(gremlinEdge);
                    var query = client.CreateGremlinQuery<dynamic>(graph, gremlinEdge);
                    while (query.HasMoreResults)
                    {
                        var sw = new Stopwatch();
                        sw.Start();
                        query.ExecuteNextAsync().Wait();
                        Console.WriteLine($"Vertex created in: {s.ElapsedMilliseconds} ms");
                    }
                    Console.WriteLine();
                })
            );
            Console.WriteLine($"Total Time: {s.ElapsedMilliseconds} ms");
        }

        private static IDriver CreateNeo4JDriver()
        {
            var noSsl = new Config { EncryptionLevel = EncryptionLevel.None };
            return GraphDatabase.Driver(_neoBolt, AuthTokens.Basic(_neoUser, _neoPass), noSsl);
        }

        private static IEnumerable<INode> ReadNeoVertexes()
        {
            var returnResult = new List<INode>();

            using (var driver = CreateNeo4JDriver())
            using (var session = driver.Session())
            {
                var result = session.Run("MATCH (n) RETURN n");
                returnResult.AddRange(result.Select(record => record["n"].As<INode>()));
            }
            return returnResult;
        }

        private static IEnumerable<IRelationship> ReadNeoEdges()
        {
            var returnResult = new List<IRelationship>();

            using (var driver = CreateNeo4JDriver())
            using (var session = driver.Session())
            {
                var result = session.Run("MATCH (a)-[r]->(b) RETURN r");
                returnResult.AddRange(result.Select(record => record["r"].As<IRelationship>()));
            }
            return returnResult;
        }

        private static string ConvertItemToGremlinVertex(INode item)
        {
            var vertex = $"g.addV('{item.Labels[0]}')";
            vertex += $".property('id', '{item.Id}')";
            foreach (var prop in item.Properties)
            {
                var propValue = prop.Value.ToString().Replace("'", "");
                vertex += $".property('{prop.Key}', '{propValue}')";
            }
            return vertex;
        }

        private static string ConvertItemToGremlinEdge(IRelationship item)
        {
            var edge = $"g.V('{item.StartNodeId}')";
            edge += $".addE('{item.Type}')";
            foreach (var prop in item.Properties)
            {
                var propValue = prop.Value.ToString().Replace("'", "");
                edge += $".property('{prop.Key}', '{propValue}')";
            }
            edge += $".to(g.V('{item.EndNodeId}'))";
            return edge;
        }
    }
}
