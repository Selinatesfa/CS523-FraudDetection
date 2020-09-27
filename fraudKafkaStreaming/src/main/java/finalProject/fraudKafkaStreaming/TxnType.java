package finalProject.fraudKafkaStreaming;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;

public enum TxnType {
 PAYMENT,TRANSFER,CASH_OUT,CASH_IN,DEBIT,CREDIT;
 
 
 private static final List<TxnType> VALUES = Collections.unmodifiableList(Arrays.asList(values()));
private static final int SIZE = VALUES.size();
private static final Random RANDOM = new Random();

public static TxnType randomTxnType()  {
 return VALUES.get(RANDOM.nextInt(SIZE));
}
}
