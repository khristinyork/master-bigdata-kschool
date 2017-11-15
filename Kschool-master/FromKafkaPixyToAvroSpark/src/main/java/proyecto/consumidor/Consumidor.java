package proyecto.consumidor;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import com.twitter.bijection.Injection;
import com.twitter.bijection.avro.GenericAvroCodecs;

import proyecto.jsonweather.Weather;
import proyecto.serializersAnDtranformers.JsonTransformer;

public class Consumidor {

	public static String KAFKA_HOST="172.31.20.14:9090,172.31.20.14:9091,172.31.20.14:9092";
	public static String TOPIC="datos";
	public static String TOPIC_WEATHER_AVRO="wtopic";
	public static String GROUPID="grupo1";
	private static final AtomicBoolean closed = new AtomicBoolean(false);
	

	public final static String WEATHER_SCHEMA = "{" + "\"type\":\"record\"," + "\"name\":\"myrecord\"," + "\"fields\":["
			+ "{\"name\":\"lon\",\"type\":\"int\"}," + "{\"name\":\"lat\",\"type\":\"int\"},"
			+ "{\"name\":\"temp\",\"type\":\"int\"}," + "{\"name\":\"pressure\",\"type\":\"int\"},"
			+ "{\"name\":\"humidity\",\"type\":\"int\"}," + "{\"name\":\"temp_min\",\"type\":\"int\"},"
			+ "{\"name\":\"temp_max\",\"type\":\"int\"}," + "{\"name\":\"id\",\"type\":\"int\"},"
			+ "{\"name\":\"datetime\",\"type\":\"int\"}" + "]" + "}";

	public static void inicializacion() {
		Properties prop = new Properties();
		OutputStream output = null;

		try {
			output = new FileOutputStream("kafka.properties");

			// set the properties value
			KAFKA_HOST = (String) prop.get("kafka.host");
			TOPIC = (String) prop.get("topic.kafkapixy");
			TOPIC_WEATHER_AVRO = (String) prop.get("topic.avro.to.spark");
			GROUPID = (String) prop.get("group.id");
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	public static void main(String[] args) {
		Runtime.getRuntime().addShutdownHook(new Thread() {
			@Override
			public void run() {
				System.out.println("Shutting down");
				closed.set(true);
			}
		});

		//inicializacion();
		Properties props = new Properties();
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_HOST);
		props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
		// props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
		props.put(ConsumerConfig.GROUP_ID_CONFIG, "grupo1");
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
				"org.apache.kafka.common.serialization.StringDeserializer");
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
				"org.apache.kafka.common.serialization.StringDeserializer");

		KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);
		consumer.subscribe(Collections.singletonList(TOPIC));
		String value = "",key="";
		Weather w, weatherSend = null;
		Properties props1 = new Properties();
		props1.put("bootstrap.servers", KAFKA_HOST);
		props1.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props1.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");

		Schema.Parser parser = new Schema.Parser();
		// System.out.println(WEATHER_SCHEMA);
		Schema schema = parser.parse(WEATHER_SCHEMA);
		Injection<GenericRecord, byte[]> recordInjection = GenericAvroCodecs.toBinary(schema);
		KafkaProducer<String, byte[]> producer = new KafkaProducer<String, byte[]>(props1);
		try {
			while (!closed.get()) {

				ConsumerRecords<String, String> records = consumer.poll(1000);
				for (ConsumerRecord<String, String> record : records) {
					value = record.value();
					key=record.key();
					System.out.println("record2: "+value);
					w = new Weather();
					weatherSend = JsonTransformer.transformerToweatherSchema(record.value(), w);
					GenericData.Record avroRecord = new GenericData.Record(schema);
					avroRecord.put("lon", weatherSend.getLon());
					avroRecord.put("lat", weatherSend.getLat());
					avroRecord.put("temp", weatherSend.getTemp());
					avroRecord.put("pressure", weatherSend.getPressure());
					avroRecord.put("humidity", weatherSend.getHumidity());
					avroRecord.put("temp_min", weatherSend.getTempMin());
					avroRecord.put("temp_max", weatherSend.getTempMax());
					avroRecord.put("id", weatherSend.getId());
					avroRecord.put("datetime", weatherSend.getDatatime());
					System.out.println("avroRecord: "+avroRecord.get("id"));
					byte[] bytes = recordInjection.apply(avroRecord);
					
					ProducerRecord<String, byte[]> record2 = new ProducerRecord<String, byte[]>(TOPIC_WEATHER_AVRO,key,bytes);
					producer.send(record2);
					
					Thread.sleep(1000);

				}
			}
		} catch (Exception e) {
			System.out.println(Consumidor.class+" Error al leer de topic dato al enviar a wtopic: "+e.getMessage());
		} finally {
			producer.flush();
			producer.close();
			consumer.close();
		}

	}
}
