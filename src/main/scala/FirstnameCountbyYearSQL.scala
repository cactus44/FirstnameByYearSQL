/**
  * Created by guillaume on 06/02/17.
  */


import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.apache.spark.sql.functions._

object FirstnameCountbyYearSQL {

  def main(args: Array[String]) = {

    val logFile = "/home/guillaume/TP/Spark/dpt2015.txt"
    // Should be some file on your system
    val conf = new SparkConf().setAppName("Simple Application").setMaster("local")
    val sc = new SparkContext(conf)
    //pour chargement dynamique faire un SparkConf vide
    val logData = sc.textFile(logFile, 2).cache()

    // Conversion fichier au format TSV en fichier au format CSV
    /*
        val csv = logData.map(_.split("\t")).map(c => c(1)+ "," + c(2).replace("XXXX","0") +"," + c(3).replace("XX","0") +","+ c(4).replace(".0000", "").toInt)
        csv.saveAsTextFile("/home/guillaume/TP/Spark/dpt2015.txt.csv")
    */


    val spark = SparkSession
      .builder().master("local")
      .appName("Spark SQL basic example")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()

    // Définition du schéma SQL correspondant au fichier

    val prenom = StructField("prenom", DataTypes.StringType)
    val annee = StructField("annee", DataTypes.IntegerType)
    val departement = StructField("departement", DataTypes.IntegerType)
    val nombre = StructField("nombre", DataTypes.IntegerType)

    val fields = Array(prenom, annee, departement, nombre)
    val schema = StructType(fields)

    val df = spark.read.schema(schema).csv("/home/guillaume/TP/Spark/dpt2015.txt.csv")

    df.printSchema()
    df.createOrReplaceTempView("people")

    val toto = spark.sql("SELECT * FROM people WHERE departement = 44")
    toto.show(100)


   val sums = toto.agg(sum("nombre")).first.get(0)
    println("TOTAL : il y a " + sums + " occurences")

  }
}

