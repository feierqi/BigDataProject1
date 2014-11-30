package com.hadoop.isp;

import java.io.PrintWriter;

public class CreateTransactions {
	private PrintWriter writer;
	
	public CreateTransactions(PrintWriter writer){
		this.writer = writer;
	}
	
	public void generateTransactions(){
		for(int i = 1; i <= 5000000; i++){
			final int transID = i;
			final int custID = RandomGenerator.randInt(1, 50000);
			final float trasTotal = RandomGenerator.randFloat(10.0f, 1000.0f);
			final int transNumItems = RandomGenerator.randInt(1, 10);
			final String transDesc = RandomGenerator.randomString(20, 50);
			writer.println(transID + "," + custID + "," + trasTotal + "," + transNumItems + "," + transDesc);
		}
	}
}
