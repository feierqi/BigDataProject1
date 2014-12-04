javac -classpath ${HADOOP_HOME}/hadoop-core-1.1.0.jar -d . ReportCountryCode.java
jar -cvf ../../ReportCountryCode.jar . .
cd ../..
bin/hadoop jar ReportCountryCode.jar ReportCountryCode /tmp/Customers.txt ./out3

hadoop fs -rmr /user/ubuntu/

	
	
