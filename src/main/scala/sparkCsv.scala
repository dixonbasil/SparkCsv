import org.apache.spark.sql.SparkSession
object sparkCsv{
  def main(args: Array[String]): Unit = {
    val spark=SparkSession.builder()
      .master("local[*]")
      .appName("sparkCSV")
      .getOrCreate()
//    val sc=spark.sparkContext
    val df=spark.read.format("csv").options(Map("header"->"true","inferSchema"->"true")).load("/home/uvionics/test.csv")
    df.show()
    df.createGlobalTempView("table")
    spark.sql("SELECT * FROM global_temp.table WHERE table.Age<28").show()
//    df.show()
//    df.printSchema()

  }
}