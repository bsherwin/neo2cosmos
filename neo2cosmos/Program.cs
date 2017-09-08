using System;
using System.Collections.Generic;
using System.Configuration;
using System.Threading.Tasks;
using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;
using Microsoft.Azure.Documents.Linq;
using Microsoft.Azure.Graphs;
using Neo4j.Driver.V1;

namespace neo2cosmos
{
    public class Program
    {
        private static string CosmosEndpoint;
        private static string CosmosAuthKey;
        private static string NeoBolt;
        private static string NeoUser;
        private static string NeoPass;

        public static void Main(string[] args)
        {
            CosmosEndpoint = ConfigurationManager.AppSettings["CosmosEndpoint"];
            CosmosAuthKey = ConfigurationManager.AppSettings["CosmosAuthKey"];
            NeoBolt = ConfigurationManager.AppSettings["NeoBolt"];
            NeoUser = ConfigurationManager.AppSettings["NeoUser"];
            NeoPass = ConfigurationManager.AppSettings["NeoPass"];

            using (DocumentClient client = new DocumentClient(
                 new Uri(CosmosEndpoint),
                 CosmosAuthKey,
                 new ConnectionPolicy { ConnectionMode = ConnectionMode.Direct, ConnectionProtocol = Protocol.Tcp }))
            {
                Program p = new Program();
                p.RunAsync(client).Wait();
            }
        }

        public async Task RunAsync(DocumentClient client)
        {
            Database database = await client.CreateDatabaseIfNotExistsAsync(new Database { Id = "graphdb" });
            DocumentCollection graph = await client.CreateDocumentCollectionIfNotExistsAsync(
                UriFactory.CreateDatabaseUri("graphdb"),
                new DocumentCollection { Id = "Northwind" },
                new RequestOptions { OfferThroughput = 400 });

            await client.OpenAsync();
            await CleanupFromLastExecute(client, graph);
            await CreateVertexes(client, graph);
            await CreateEdges(client, graph);
        }

        private async Task CleanupFromLastExecute(DocumentClient client, DocumentCollection graph)
        {
            IDocumentQuery<dynamic> cleanup = client.CreateGremlinQuery<dynamic>(graph, "g.V().drop()");
            await cleanup.ExecuteNextAsync();
        }

        private async Task CreateVertexes(DocumentClient client, DocumentCollection graph)
        {
            foreach (var item in ReadNeoVertexes())
            {
                string gremlinVertex = ConvertItemToGremlinVertex(item);
                Console.WriteLine(gremlinVertex);
                IDocumentQuery<dynamic> query = client.CreateGremlinQuery<dynamic>(graph, gremlinVertex);
                while (query.HasMoreResults)
                {
                    await query.ExecuteNextAsync();
                }

                Console.WriteLine();
            }

        }

        private async Task CreateEdges(DocumentClient client, DocumentCollection graph)
        {
            foreach (var item in ReadNeoEdges())
            {
                string gremlinEdge = ConvertItemToGremlinEdge(item);
                Console.WriteLine(gremlinEdge);
                IDocumentQuery<dynamic> query = client.CreateGremlinQuery<dynamic>(graph, gremlinEdge);
                while (query.HasMoreResults)
                {
                    await query.ExecuteNextAsync();
                }
                Console.WriteLine();
            }
        }

        private List<INode> ReadNeoVertexes()
        {
            var returnResult = new List<INode>();
            Config noSSL = new Config();
            noSSL.EncryptionLevel = EncryptionLevel.None;

            using (var driver = GraphDatabase.Driver(NeoBolt, AuthTokens.Basic(NeoUser, NeoPass), noSSL))
            using (var session = driver.Session())
            {
                var result = session.Run("MATCH (n) RETURN n");
                foreach (var record in result)
                {
                    returnResult.Add(record["n"].As<INode>());
                }
            }
            return returnResult;
        }

        private List<IRelationship> ReadNeoEdges()
        {
            var returnResult = new List<IRelationship>();
            Config noSSL = new Config();
            noSSL.EncryptionLevel = EncryptionLevel.None;

            using (var driver = GraphDatabase.Driver(NeoBolt, AuthTokens.Basic(NeoUser, NeoPass), noSSL))
            using (var session = driver.Session())
            {
                var result = session.Run("MATCH (a)-[r]->(b) RETURN r");
                foreach (var record in result)
                {
                    returnResult.Add(record["r"].As<IRelationship>());
                }
            }
            return returnResult;
        }

        private string ConvertItemToGremlinVertex(INode item)
        {
            string vertex = string.Format("g.addV('{0}')", item.Labels[0]);
            vertex += string.Format(".property('id', '{0}')", item.Id);
            foreach (var prop in item.Properties)
            {
                string propValue = prop.Value.ToString().Replace("'", "");
                vertex += string.Format(".property('{0}', '{1}')", prop.Key, propValue);
            }
            return vertex;
        }

        private string ConvertItemToGremlinEdge(IRelationship item)
        {
            string edge = string.Format("g.V('{0}')", item.StartNodeId);
            edge += string.Format(".addE('{0}')", item.Type);
            foreach (var prop in item.Properties)
            {
                string propValue = prop.Value.ToString().Replace("'", "");
                edge += string.Format(".property('{0}', '{1}')", prop.Key, propValue);
            }
            edge += string.Format(".to(g.V('{0}'))", item.EndNodeId);
            return edge;
        }

    }
}
