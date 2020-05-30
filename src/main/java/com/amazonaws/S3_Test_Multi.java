package com.amazonaws;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;



public class S3_Test_Multi {

	static AWSProp prop = new AWSProp();
	public static List<File> uploadFileList = new ArrayList<File>();
	
	public static void main(String[] args) throws Exception {
		// upload 대상 파일 리스트
		uploadFileList.add(new File("C:\\s3_test\\multi_thread\\test01.txt"));
		uploadFileList.add(new File("C:\\s3_test\\multi_thread\\test02.txt")); 
		uploadFileList.add(new File("C:\\s3_test\\multi_thread\\test03.txt")); 
		uploadFileList.add(new File("C:\\s3_test\\multi_thread\\test04.txt")); 
		uploadFileList.add(new File("C:\\s3_test\\multi_thread\\test05.txt")); 
		uploadFileList.add(new File("C:\\s3_test\\multi_thread\\test06.txt")); 
		uploadFileList.add(new File("C:\\s3_test\\multi_thread\\test07.txt")); 
		uploadFileList.add(new File("C:\\s3_test\\multi_thread\\test08.txt")); 
		uploadFileList.add(new File("C:\\s3_test\\multi_thread\\test09.txt")); 
		uploadFileList.add(new File("C:\\s3_test\\multi_thread\\test10.txt")); 
		uploadFileList.add(new File("C:\\s3_test\\multi_thread\\test11.txt")); 
		uploadFileList.add(new File("C:\\s3_test\\multi_thread\\test12.txt")); 
		uploadFileList.add(new File("C:\\s3_test\\multi_thread\\test13.txt")); 
		uploadFileList.add(new File("C:\\s3_test\\multi_thread\\test14.txt")); 
		uploadFileList.add(new File("C:\\s3_test\\multi_thread\\test15.txt")); 
		uploadFileList.add(new File("C:\\s3_test\\multi_thread\\test16.txt")); 
		uploadFileList.add(new File("C:\\s3_test\\multi_thread\\test17.txt")); 
		uploadFileList.add(new File("C:\\s3_test\\multi_thread\\test18.txt")); 
		uploadFileList.add(new File("C:\\s3_test\\multi_thread\\test19.txt")); 
		uploadFileList.add(new File("C:\\s3_test\\multi_thread\\test20.txt")); 
		uploadFileList.add(new File("C:\\s3_test\\multi_thread\\test21.txt")); 
		uploadFileList.add(new File("C:\\s3_test\\multi_thread\\test22.txt")); 
		uploadFileList.add(new File("C:\\s3_test\\multi_thread\\test23.txt")); 
		uploadFileList.add(new File("C:\\s3_test\\multi_thread\\test24.txt")); 
		uploadFileList.add(new File("C:\\s3_test\\multi_thread\\test25.txt")); 
		uploadFileList.add(new File("C:\\s3_test\\multi_thread\\test26.txt")); 
		uploadFileList.add(new File("C:\\s3_test\\multi_thread\\test27.txt")); 
		uploadFileList.add(new File("C:\\s3_test\\multi_thread\\test28.txt")); 
		uploadFileList.add(new File("C:\\s3_test\\multi_thread\\test29.txt")); 
		uploadFileList.add(new File("C:\\s3_test\\multi_thread\\test30.txt")); 

		// upload 실행
		uploadOnMultiThread();

		
		System.exit(0);
	}
	
	
	protected static void uploadOnMultiThread() {
		
		// util 클래스 객체는 싱글튼으로 생성
		final S3Util s3Util = S3Util.getInstance();
		s3Util.setAWSProp(prop);
		s3Util.setAWSAuth();
		s3Util.createBucket(prop.getBucketName1());
		
		
		// thread pool 생성; 
		int thread_count = 5;
		
		ExecutorService ioThreadPool = Executors.newFixedThreadPool(thread_count);
		
		List<Future<Integer>> threadList = new ArrayList<Future<Integer>>();
		
		for (int i = 0; i <(thread_count * 2); i++) {
			UploadTaskThread utt = new UploadTaskThread();
			
			utt.init(s3Util);
			Future<Integer> future = ioThreadPool.submit(utt);
			
			threadList.add(future);
			
		}
		
		try {
			for (Future<Integer> future : threadList) {
				int res = future.get();
				System.out.println("스레드 작업 실행 결과 : [ "+res+" ]");
			}
		} catch (Exception e) {
			e.printStackTrace();
		} 
		
		
	}
	
	protected static File fetchFileList() {
		File f = null;
		
		synchronized(uploadFileList) {
			if (uploadFileList.size() > 0) {
				f = uploadFileList.get(0);
				uploadFileList.remove(0);
			}
		}
		
		return f;
	}
	
}
