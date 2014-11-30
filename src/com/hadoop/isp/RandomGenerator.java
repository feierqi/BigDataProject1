package com.hadoop.isp;

import java.util.Random;

public class RandomGenerator {
	
	private static final String CHAR_LIST = "abcdefghijklmnopqrstuvwxyz";
	
	public static int randInt(int min, int max) {

	    // NOTE: Usually this should be a field rather than a method
	    // variable so that it is not re-seeded every call.
	    Random rand = new Random();

	    // nextInt is normally exclusive of the top value,
	    // so add 1 to make it inclusive
	    int randomNum = rand.nextInt((max - min) + 1) + min;

	    return randomNum;
	}
	
	public static float randFloat(float min, float max) {

	    Random rand = new Random();

	    float randomNum = rand.nextFloat() * (max - min) + min;

	    return randomNum;
	}
	
    public static String randomString(int min, int max){
        
        StringBuffer randStr = new StringBuffer();
        int length = randInt(min, max);
        for(int i = 0; i < length; i++){
            int number = randInt(0, 25);
            char ch = CHAR_LIST.charAt(number);
            randStr.append(ch);
        }
        return randStr.toString();
    }
}
