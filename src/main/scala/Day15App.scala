import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Day15App {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Day15-UDF")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    import spark.implicits._

    val employees = Seq(
      ("E1", "Ali", 45000),
      ("E2", "Rahul", 75000),
      ("E3", "Sara", 120000),
      ("E4", "John", 55000)
    ).toDF("id", "name", "salary")

    val salaryBand = udf((salary: Double) => {
      if (salary >= 100000) "PREMIUM"
      else if (salary >= 70000) "HIGH"
      else if (salary >= 50000) "MEDIUM"
      else "LOW"
    })

    val result = employees.withColumn(
      "salary_band",
      salaryBand($"salary")
    )

    result.show()

    spark.udf.register(
      "salary_band_sql",
      (salary: Double) => {
        if (salary >= 100000) "PREMIUM"
        else if (salary >= 70000) "HIGH"
        else if (salary >= 50000) "MEDIUM"
        else "LOW"
      }
    )

    result.createOrReplaceTempView("employees")

    spark.sql(
      """
        |SELECT name, salary,
        |salary_band_sql(salary) AS band
        |FROM employees
        |""".stripMargin
    ).show()

    spark.stop()
  }
}
