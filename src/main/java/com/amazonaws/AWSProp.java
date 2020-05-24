package com.amazonaws;

import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class AWSProp {

	private final String propertiesPath = System.getProperty("user.dir")+"/src/main/resources/aws.properties";

	private String accessKey;
	private String secretKey;
	private String bucketName1;
	private String bucketName2;
	
	public AWSProp() {
		Properties prop = new Properties();
		FileReader reader = null;
		
		try {
			reader = new FileReader(propertiesPath);
			prop.load(reader);

			accessKey = prop.getProperty("accesskey");
			secretKey = prop.getProperty("secretkey");
			bucketName1 = prop.getProperty("bucketname1");
			bucketName2 = prop.getProperty("bucketname2");
			
			System.out.println(toString());
			
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public String getAccessKey() {
		return accessKey;
	}

	public String getSecretKey() {
		return secretKey;
	}

	public String getBucketName1() {
		return bucketName1;
	}

	public String getBucketName2() {
		return bucketName2;
	}

	@Override
	public String toString() {
		return "AWSProp [propertiesPath=" + propertiesPath + ", accessKey=" + accessKey + ", secretKey=" + secretKey
				+ ", bucketName1=" + bucketName1 + ", bucketName2=" + bucketName2 + "]";
	}
	
	

}
