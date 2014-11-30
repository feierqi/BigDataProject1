package com.hadoop.isp;

import java.io.PrintWriter;

public class CreateCustomers {
	
	private PrintWriter writer;
	
	public CreateCustomers(PrintWriter writer){
		this.writer = writer;
	}
	
	public void generateCustomers(){
		for(int i = 1; i <= 50000; i++){
			final int ID = i;
			final String name = RandomGenerator.randomString(10, 20);
			final int age = RandomGenerator.randInt(10, 70);
			final int countryCode = RandomGenerator.randInt(1, 10);
			final float salary = RandomGenerator.randFloat(100.0f, 10000.0f);
			writer.println(ID + "," + name + "," + age + "," + countryCode + "," + salary);
		}
	}
}
