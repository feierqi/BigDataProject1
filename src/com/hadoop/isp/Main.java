package com.hadoop.isp;

import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;

public class Main {
	public static void main(String[] argv) {
		PrintWriter custWriter = null;
		try {
			custWriter = new PrintWriter("Customers.txt", "UTF-8");
		} catch (FileNotFoundException | UnsupportedEncodingException e) {
			System.out.println(e.getMessage());
		}
		PrintWriter transWriter = null;
		try {
			transWriter = new PrintWriter("Transactions.txt", "UTF-8");
		} catch (FileNotFoundException | UnsupportedEncodingException e) {
			System.out.println(e.getMessage());
		}
		CreateCustomers createCustomers = new CreateCustomers(custWriter);
		CreateTransactions createTransactions = new CreateTransactions(transWriter);
		createCustomers.generateCustomers();
		createTransactions.generateTransactions();
		custWriter.close();
		transWriter.close();
	}
}
