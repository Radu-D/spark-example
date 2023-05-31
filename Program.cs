using System;
using Microsoft.Spark;
using Microsoft.Spark.Sql;

//using Microsoft.Spark;

namespace SparkExample
{
    public static class Program
    {
        public static void Main(string[] args)
        {
            var spark = SparkSession
                .Builder()
                //.Master("local[*]")
                // .Master("192.168.2.195:49571")
                .Master("spark://ca1df3e51fb2:7077")
                .AppName("connectors")
                .Config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
                .Config("spark.eventLog.enabled", "true")
                .Config("spark.eventLog.dir", "/home/iceberg/spark-events")
                .Config("spark.history.fs.logDirectory", "/home/iceberg/spark-events")
                .GetOrCreate();

            spark.SparkContext.SetLogLevel("ERROR");
            
            // Mock data from https://mockaroo.com/
            var df = spark.Read().Json("facilities.json");
            df.Show();
            df.Write().Parquet("nom_fichier00.parquet");
            
            // var spark = SparkSession.Builder().AppName("Connectors").Config(new SparkConf()).Master("spark://ca1df3e51fb2:7077").GetOrCreate();
            // var df = spark.Read().Json("brunetPharmacies.json");
            // df.Show();
            //
            // var t = "";
            //var df = spark.CreateDataFrame();
            //df.Show();
            //df.Write().Parquet("nom_fichier.parquet");
        }
    }
}