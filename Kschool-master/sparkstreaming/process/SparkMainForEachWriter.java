package spark.process;

import com.databricks.spark.avro.SchemaConverters;
import com.twitter.bijection.Injection;
import com.twitter.bijection.avro.GenericAvroCodecs;

import org.apache.spark.sql.streaming.*;
import java.util.concurrent.TimeUnit;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.SparkContext;


public class SparkMainForEachWriter {

	
	// public static String IPKAFKA="172.31.20.14";
	
	
	//Mongo
	private static String mongodbIp = "192.168.1.225";
	private static String dbName = "wdb";
	private static String collectionName = "wtable";	
	//Spark
	static String windowDuration = 2 + " seconds";// 30 minutos
	static String slideDuration = 1 + " seconds";// 1 minutos
	//Kafka
	public static String IPKAFKA = "34.224.28.222";
	public static String KAFKA_HOST = IPKAFKA + ":9090," + IPKAFKA + ":9091," + IPKAFKA + ":9092";
	public static String TOPIC = "wtopic";
	public static String TOPIC2MONGO = "topicFromKafkaToMongo";
	
	
	//esquema Avro en el que viene el value del topic kafka
	public final static String WEATHER_SCHEMA = "{" + "\"type\":\"record\"," + "\"name\":\"myrecord\"," + "\"fields\":["
			+ "{\"name\":\"lon\",\"type\":\"int\"}," + "{\"name\":\"lat\",\"type\":\"int\"},"
			+ "{\"name\":\"temp\",\"type\":\"int\"}," + "{\"name\":\"pressure\",\"type\":\"int\"},"
			+ "{\"name\":\"humidity\",\"type\":\"int\"}," + "{\"name\":\"temp_min\",\"type\":\"int\"},"
			+ "{\"name\":\"temp_max\",\"type\":\"int\"}," + "{\"name\":\"id\",\"type\":\"int\"},"
			+ "{\"name\":\"datetime\",\"type\":\"int\"}" + "]" + "}";
	
	//Parser de esquema
	private static Schema.Parser parser = new Schema.Parser();
	
	//Se carga el schema al que se parsea
	private static Schema schema = parser.parse(WEATHER_SCHEMA);
	
    //Cargamos el esquema en el registro de avro
	private static Injection<GenericRecord, byte[]> recordInjection;
	private static StructType type;
	static {
		recordInjection = GenericAvroCodecs.toBinary(schema);
		type = (StructType) SchemaConverters.toSqlType(schema).dataType();

	}

	public static void main(String[] args) throws StreamingQueryException, Exception {
		
		//Instanciamos y configuramos  spark
		SparkConf conf = new SparkConf().setAppName("sparkprocess").setMaster("local[*]");
		SparkContext sparkContext = new SparkContext(conf);
		sparkContext.setCheckpointDir("/media/bigdata/sda6/opt/HDFS");
		SparkSession sparkSession = SparkSession.builder().config(conf).getOrCreate();
	    sparkSession.sqlContext().setConf("spark.sql.shuffle.partitions", "3");
	    //Leemos desde kafka los datos y los cargamos en un dataset
		Dataset<Row> ds1 = sparkSession.readStream().format("kafka").option("kafka.bootstrap.servers", KAFKA_HOST)
				.option("subscribe", TOPIC).option("startingOffsets", "earliest").load();
		//Creamos una funcion de usuario que deserializa los datos y los formatea a un schema avro 
		sparkSession.udf().register("deserialize", (byte[] data) -> {
			GenericRecord record = recordInjection.invert(data).get();
			return RowFactory.create(record.get("id"), record.get("lon"), record.get("lat"), record.get("temp"),
					record.get("pressure"), record.get("humidity"), record.get("temp_min"), record.get("temp_max"),
					record.get("datetime"));

		}, DataTypes.createStructType(type.fields()));
        //El dataset de filas le aplicamos la udf deserialize
		Dataset<Row> ds2 = ds1.select("value").as(Encoders.BINARY()).selectExpr("deserialize(value) as rows").select("rows.*");

		ds2.printSchema();
	    
	
		Dataset<Row> ds4 = ds2.groupBy(functions.window(ds2.col("datetime").cast(("Timestamp")),windowDuration,slideDuration),ds2.col("id"),ds2.col("lon"),ds2.col("lat")).agg(
				org.apache.spark.sql.functions.avg(ds2.col("humidity")),
				org.apache.spark.sql.functions.avg(ds2.col("pressure")),
				org.apache.spark.sql.functions.min(ds2.col("temp")),
				org.apache.spark.sql.functions.max(ds2.col("temp"))
				);
	
		ds4.printSchema();
		StreamingQuery query2 = ds4
				 .select(ds4.col("id"),ds4.col("max(temp)"),
						 ds4.col("lon"),ds4.col("lat"),ds4.col("min(temp)"),
						 ds4.col("avg(pressure)"),ds4.col("avg(humidity)"),ds4.col("window.start"))
				.writeStream()
				.foreach(new InserterForEach())
				.outputMode(OutputMode.Complete())
			    .trigger(ProcessingTime.create(1, TimeUnit.SECONDS)).start();
		
		query2.awaitTermination();
	}
}
