package finalProject.fraudKafkaStreaming;
import com.fasterxml.jackson.databind.ObjectMapper;
import kafka.serializer.Decoder;
import kafka.utils.VerifiableProperties;

public class TxnInfoDecoder implements Decoder<TransactionInfo> {

	public TxnInfoDecoder(VerifiableProperties verifiableProperties) {
	}

	private static ObjectMapper objectMapper = new ObjectMapper();
	@Override
	public TransactionInfo fromBytes(byte[] bytes) {
		try {
			return objectMapper.readValue(bytes, TransactionInfo.class);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}
	
	
}
