package finalProject.fraudKafkaStreaming;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.UUID;

import org.apache.log4j.Logger;


import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;


public class FraudProducer {
	private static final Logger logger = Logger.getLogger(FraudProducer.class);

	public static void main(String[] args) throws Exception {
		
		String zookeeper ="localhost:2181";
		String brokerList="localhost:9092";
		String topic ="fraud-detection";
		
		logger.info("Using Zookeeper=" + zookeeper + " ,Broker-list=" + brokerList + " and topic " + topic);
		System.out.println("Using Zookeeper=" + zookeeper + " ,Broker-list=" + brokerList + " and topic " + topic);

		// set producer properties
		Properties properties = new Properties();
		properties.put("zookeeper.connect", zookeeper);
		properties.put("metadata.broker.list", brokerList);
		properties.put("request.required.acks", "1");
		properties.put("serializer.class", "finalProject.fraudKafkaStreaming.TxnInfoEncoder");
		//generate event
		Producer<String, TransactionInfo> producer = new Producer<String, TransactionInfo>(new ProducerConfig(properties));
		FraudProducer fraudProducer = new FraudProducer();
		fraudProducer.generateIoTEvent(producer,topic);		
	}



	private void generateIoTEvent(Producer<String, TransactionInfo> producer, String topic) throws InterruptedException {
	
		Random random = new Random();
		
		logger.info("Sending events");
		// generate event in loop
		while (true) {
			List<TransactionInfo> eventList = new ArrayList<TransactionInfo>();
			for (int i = 0; i < 100; i++) {// create 100 Transaction
			
				TransactionInfo info = new TransactionInfo();
		    	int orig = random.nextInt(100000000);
		    	int dest = random.nextInt(100000000);
		    	double amount = random.nextInt(1500000);
		    	String destName = dest % 2 == 0 ? "M" : "C";
		    	info.setType(TxnType.randomTxnType().name());
		    	info.setAmount(amount);
		    	info.setNameOrig("C"+orig);
		    	info.setNameDest(destName+dest);
		    	eventList.add(info);
				
			}
			Collections.shuffle(eventList);// shuffle for random events
			for (TransactionInfo event : eventList) {
				KeyedMessage<String, TransactionInfo> data = new KeyedMessage<String, TransactionInfo>(topic, event);
				//System.out.println(data.toString());
				producer.send(data);
				Thread.sleep(random.nextInt(3000 - 1000) + 1000);//random delay of 1 to 3 seconds
			}
		}
	}
	
	
}
