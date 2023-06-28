using System.Diagnostics;
using Apache.Arrow.Types;
using Microsoft.Spark;
using Microsoft.Spark.Sql;
using Microsoft.Spark.Sql.Streaming;
using Microsoft.Spark.Sql.Types;
using Newtonsoft.Json.Linq;
using TrinoClient;
using TrinoClient.Model.Statement;
using StringType = Microsoft.Spark.Sql.Types.StringType;
using StructType = Microsoft.Spark.Sql.Types.StructType;

namespace SparkExample
{
    public static class Program
    {
        public static async Task Main(string[] args)
        {
            // if (Environment.GetEnvironmentVariable("DOTNET_WORKER_DEBUG")?.Equals("1") == true)
            // {
            Debugger.Launch();
            //}
            
            try
            {
                GetEachTableFromResourcesBronzeIceberg();
                //await TrinoSelect();
                //MakeABrunetTableUsingSpark();
                //TrinoInsert();
            }
            catch (Exception e)
            {
                Console.WriteLine(e);
                throw;
            }
            
        }

        private static void GetEachTableFromResourcesBronzeIceberg()
        {
            var sparkConf = new SparkConf();

            var spark = SparkSession
                .Builder()
                .Master("local[*]")
                .AppName("connectors")
                .Config(sparkConf)
                .EnableHiveSupport()
                .GetOrCreate();

            spark.SparkContext.SetLogLevel("ERROR");

            var allTablesFromResourcesBronze = spark.Sql("SHOW TABLES FROM resources_bronze");
            var tableNames = allTablesFromResourcesBronze.Select("tableName").As("Table");
            tableNames.Show();
            
            // Get first column (the table name) from each returned row
            var tableNamesAsStrings = tableNames.Collect().Select(x => x.Values.First());
            foreach (var tableName in tableNamesAsStrings)
            {
                var entriesFromResourcesBronze = spark.Sql($"SELECT * FROM resources_bronze.{tableName}");
                entriesFromResourcesBronze.Show();
                entriesFromResourcesBronze.Write()
                    .Mode("append")
                    .Json($"./resources_bronze.{tableName}.parquet");
            }
        }

        private static void MakeABrunetTableUsingSpark()
        {
            var sparkConf = new SparkConf();

            var spark = SparkSession
                .Builder()
                .Master("local[*]")
                .AppName("connectors")
                .Config(sparkConf)
                .EnableHiveSupport()
                .GetOrCreate();

            spark.SparkContext.SetLogLevel("ERROR");
            
            // Mock data from https://mockaroo.com/
            var df = spark
                .Read()
                .Json("./brunetPharmacies.json"); // each key = column
            
            df.Show();

            spark.Sql("CREATE DATABASE IF NOT EXISTS resources_bronze_test");
            spark.Sql("CREATE TABLE IF NOT EXISTS resources_bronze_test.brunetv2");
            
            df.Write()
                .Mode("overwrite")
                .Format("iceberg")
                .SaveAsTable("resources_bronze_test.brunetv2");
        }

        private static async Task TrinoSelect()
        {
            #region Trino

            var config = new TrinoClientSessionConfig
            {
                Host = "localhost",
                Port = 8080,
                Catalog = "iceberg",
                //Schema = "pharmacy_brunet"
            };
            
            var client = new TrinodbClient(config);
            var request = new ExecuteQueryV1Request("SELECT * FROM resources_bronze.pharmacy_brunet");
            var queryResponse = await client.ExecuteQueryV1(request);

            var dataToJson = queryResponse.DataToJson();

            var columns = queryResponse.Columns.ToList();
            var listOfRows = queryResponse.Data.ToList();

            foreach (var rows in listOfRows)
            {
                foreach (var column in columns)
                {
                    switch (column.Type)
                    {
                        case "varchar":
                            Console.WriteLine("string");
                            break;
                        default:
                            Console.WriteLine(column.Type);
                            break;
                    }

                    var element = rows.ElementAt(columns.IndexOf(column));
                    Console.WriteLine($"[{column.Name}] - {element.ToString()}");
                }
            }
            #endregion
        }

        private static void TrinoInsert()
        {
            #region Trino

            var config = new TrinoClientSessionConfig("lakehouse", "raw")
            {
                Host = "localhost",
                Port = 8080
            };
            
            var client = new TrinodbClient(config);
            var request = new ExecuteQueryV1Request("INSERT INTO connectors ()");
            var queryResponse = client.ExecuteQueryV1(request);

            #endregion
        }
    }
}