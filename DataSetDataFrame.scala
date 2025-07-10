import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
case class cust_cc (customer_id: Long, customer_name: String, customer_city: String, customer_state: String, customer_zipcode: String)

object DataSetDataFrame {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.master("local[*]").appName("Spark Dataset example").getOrCreate()
    import spark.implicits._

    val cust_ds = spark.read.format("csv").option("sep", "\t").option("inferSchema", "true").option("header","true").load("customers.txt").as[cust_cc]
    cust_ds.filter(c => c.customer_state=="CA").show()
    // cust_ds.filter(c => c.customer_district=="CA").show()
    println(cust_ds.rdd.first())
    val cust_count=cust_ds.filter(c=>{c.customer_state=="CA"}).map(c=>(c.customer_state, c.customer_zipcode)).groupBy($"_2").count()
    cust_count.show()

    val cust_df = spark.read.format("csv").option("sep", "\t").option("inferSchema", "true").option("header","true").load("customers.txt")
    cust_df.filter($"customer_state"==="CA").show()
    // cust_df.filter(col("customer_district")==="CA").show()
    println(cust_df.rdd.first())

    spark.stop()
  }
}