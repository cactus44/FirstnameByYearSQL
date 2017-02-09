/**
  * Created by guillaume on 06/02/17.
  */


import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}

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

    df.show()
    df.printSchema()
    df.createOrReplaceTempView("people")

    //df.filter(df("annee").equalTo(1978)).show()

    val toto = spark.sql("SELECT * FROM people WHERE prenom = 'GUILLAUME' AND departement = 44")

    toto.show()

    //df.select("prenom","departement")
    //df.select("preusuel").show()

  }
}

