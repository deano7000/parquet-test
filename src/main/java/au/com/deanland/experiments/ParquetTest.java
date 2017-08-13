package au.com.deanland.experiments;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import static org.apache.spark.sql.functions.col;


public class ParquetTest {
	public static void main(String [] args) {
		SparkSession spark = SparkSession
			.builder()
			.appName("Parquet buggery")
			.config("spark.master", "local") // This submits Jobs to a local temp Spark instance that gets created.
			//.master("spark://ML139378:7077")
			.getOrCreate();

		// My other theory is that if that "config" above contained a URI with a proper IP in him, it'd submit to a remote 
		// Spark cluster (assuming security was all cool, etc.)


		// DataFrame is a thing that's available in Scala and is an alias for a Dataset<Row> but has some 
		// other magic I don't get.  In Java, it looks like I'm "synthesising" a DF using Dataset<Row>.
		Dataset<Row> dataframe = spark.read().json("peeps.json");

		// Show the schema, which is extracted from the "name" fields in the JSON document.
		dataframe.printSchema();
		
		// Display the contents of the JSON doc in stdout.
		dataframe.show();

		// These bits are functions that simulate SQL-like behaviour.
		dataframe.select("name").show();
		dataframe.select(col("name"), col("salary").plus(129)).show();
		dataframe.filter(col("salary").gt(150000)).show();

		// There's also an SQL actual inline thing too.
		// You create a "table" and query it.
		dataframe.createOrReplaceTempView("peeps");
		Dataset<Row> sqlDF = spark.sql("select * from peeps");
		sqlDF.show();

		// Actually writing out a parquet file.
		dataframe.write().save("human_beans.parquet");


		Dataset<Row> directQueryResponse = spark.sql("select name from  parquet.`human_beans.parquet`");
		directQueryResponse.show();

	}
}






