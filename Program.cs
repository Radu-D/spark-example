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
        public static void Main(string[] args)
        {
            MakeABrunetTableUsingSpark();
            //TrinoInsert();
            
        }

        private static void MakeABrunetTableUsingSpark()
        {
            #region Spark

            var sparkConf = new SparkConf();

            #region data_sor config

            sparkConf.Set("spark.master", "local[*]");
            sparkConf.Set("spark.driver.memory", "30g");
            //sparkConf.Set("spark.driver.host", "127.0.0.1");
            sparkConf.Set("spark.driver.host", "192.168.1.201"); 
            sparkConf.Set("spark.driver.port", "50148");
            sparkConf.Set("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions");
            sparkConf.Set("spark.sql.defaultCatalog", "lakehouse");
            sparkConf.Set("spark.sql.catalog.lakehouse", "org.apache.iceberg.spark.SparkCatalog");
            sparkConf.Set("spark.sql.catalog.lakehouse.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog");
            sparkConf.Set("spark.sql.catalog.lakehouse.io-impl", "org.apache.iceberg.aws.s3.S3FileIO");
            sparkConf.Set("spark.sql.catalog.lakehouse.warehouse", "s3://clinia-data-lake");
            sparkConf.Set("spark.hadoop.fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
            sparkConf.Set("spark.hadoop.google.cloud.auth.service.account.enable", "true");
            //sparkConf.Set("spark.hadoop.google.cloud.auth.service.account.json.keyfile", "/opt/docker/gcp/key.json");
            sparkConf.Set("spark.hadoop.google.cloud.auth.service.account.json.keyfile", "/Users/radud/Documents/Clinia/SparkExample/key.json");//"../../../key.json");
            sparkConf.Set("spark.sql.caseSensitive", "true");
            sparkConf.Set("spark.driver.extraClassPath", "./");
            sparkConf.Set("write.data.path", "s3://clinia-data-lake/resources_bronze_test.db/");

            #endregion
            
            var spark = SparkSession
                .Builder()
                .Master("local[*]")
                // .Master("192.168.2.195:49571")
                // .Master("spark://ca1df3e51fb2:7077")
                .AppName("connectors")
                // .Config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
                .Config(sparkConf)
                .EnableHiveSupport()
                .GetOrCreate();
            
            spark.SparkContext.SetLogLevel("ERROR");
            
            // Mock data from https://mockaroo.com/
            // READ
            var df = spark
                .Read()
                // .Schema(new StructType(new List<StructField>
                // {
                //     new("raw", new StringType(), true, new JObject())
                //     // new("mapped", new StringType(), true, new JObject()),
                //     // new("timestamp", new StringType(), true, new JObject()),
                //     // new("meta", new StringType(), true, new JObject())
                // }))
                //.Json("./brunetPharmacies.json");     // each key = column
                .Text("./brunetPharmacies.json")
                .ToDF(); //"raw", "mapped", "timestamp", "meta");
            
            //df.Show();
            
            // WRITE
            // df
            //     .Write()
            //     //.Mode("append")
            //     .Mode("overwrite")
            //     //.Parquet("nom_fichier05.parquet");
            //     .Format("iceberg")
            //     //.Format("parquet")
            //     //.Parquet("s3://clinia-data-lake/resources_bronze_test.db");
            //     .Save("s3://clinia-data-lake/resources_bronze_test.db");
            
            spark.Sql("CREATE DATABASE IF NOT EXISTS resources_bronze_test");
            
            // WRITE TO TABLE
            df.Write()
                //.Mode("overwrite")
                .Format("iceberg")
                .SaveAsTable("resources_bronze_test.brunet");
            //.Save("resources_bronze_test.brunet");
            
            // Write data to the Iceberg table in streaming mode.
            // var query = df.WriteStream()
            //     .Format("iceberg")
            //     .OutputMode("append")
            //     .Trigger(Trigger.ProcessingTime(TimeSpan.FromMinutes(1).Milliseconds))
            //     .Option("path", "dlf_catalog.iceberg_db.iceberg_table")
            //     .Option("checkpointLocation", "<checkpointPath>")
            //     .Start();
            //
            // query.AwaitTermination();
            
            // var spark = SparkSession.Builder().AppName("Connectors").Config(new SparkConf()).Master("spark://ca1df3e51fb2:7077").GetOrCreate();
            // var df = spark.Read().Json("brunetPharmacies.json");
            // df.Show();
            //var df = spark.CreateDataFrame();
            //df.Show();
            //df.Write().Parquet("nom_fichier.parquet");

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