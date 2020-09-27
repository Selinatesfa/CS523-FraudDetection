package finalProject.fraudKafkaStreaming;

import kafka.serializer.Encoder;
import kafka.utils.VerifiableProperties;

import com.fasterxml.jackson.core.JsonProcessingException;

import org.apache.log4j.Logger;



import com.fasterxml.jackson.databind.ObjectMapper;

public class TxnInfoEncoder implements Encoder<TransactionInfo> {

	private static final Logger logger = Logger.getLogger(TxnInfoEncoder.class);	
	private static ObjectMapper objectMapper = new ObjectMapper();
	
	public TxnInfoEncoder(VerifiableProperties verifiableProperties ) {
		// TODO Auto-generated constructor stub
	}
	
	@Override
	public byte[] toBytes(TransactionInfo t) {
		
		try {
			String msg = objectMapper.writeValueAsString(t);
			logger.info(msg);
			return msg.getBytes();
		} catch (JsonProcessingException e) {
			logger.error("Error in Serialization", e);
		}
		return null;
	}

	
}
