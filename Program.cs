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
            var sparkConf = new SparkConf();
            
            // data_sor config
            sparkConf.Set("spark.master", "local[*]");
            sparkConf.Set("spark.driver.memory", "30g");
            sparkConf.Set("spark.driver.host", "127.0.0.1");
            sparkConf.Set("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions");
            sparkConf.Set("spark.sql.defaultCatalog", "lakehouse");
            sparkConf.Set("spark.sql.catalog.lakehouse", "org.apache.iceberg.spark.SparkCatalog");
            sparkConf.Set("spark.sql.catalog.lakehouse.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog");
            sparkConf.Set("spark.sql.catalog.lakehouse.io-impl", "org.apache.iceberg.aws.s3.S3FileIO");
            sparkConf.Set("spark.sql.catalog.lakehouse.warehouse", "s3://clinia-data-lake");
            sparkConf.Set("spark.hadoop.fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
            sparkConf.Set("spark.hadoop.google.cloud.auth.service.account.enable", "true");
            sparkConf.Set("spark.hadoop.google.cloud.auth.service.account.json.keyfile", "/Users/radud/Documents/Clinia/SparkExample/key.json");//"../../../key.json");
            sparkConf.Set("spark.sql.caseSensitive", "true");
            sparkConf.Set("spark.driver.extraClassPath", "./");

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