package finalProject.fraudKafkaStreaming;

import java.io.Serializable;

public class TransactionInfo implements Serializable {

	
	private String type;
	
	private Double amount;
	
	private String nameOrig;
	
	private String nameDest;

	public TransactionInfo (){}
	
	public TransactionInfo(String type, Double amount, String nameOrig,
			String nameDest) {
		this.type = type;
		this.amount = amount;
		this.nameOrig = nameOrig;
		this.nameDest = nameDest;
	}

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

	public Double getAmount() {
		return amount;
	}

	public void setAmount(Double amount) {
		this.amount = amount;
	}

	public String getNameOrig() {
		return nameOrig;
	}

	public void setNameOrig(String nameOrig) {
		this.nameOrig = nameOrig;
	}

	public String getNameDest() {
		return nameDest;
	}

	public void setNameDest(String nameDest) {
		this.nameDest = nameDest;
	}
	
	@Override
	public String toString() {
		// TODO Auto-generated method stub
		return type+","+amount+","+nameOrig+","+nameDest;
	}
	
}
