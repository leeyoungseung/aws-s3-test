package com.amazonaws;

import java.io.File;
import java.util.concurrent.Callable;

public class UploadTaskThread implements Callable<Integer>{

	private S3Util s3Util;
	private Object lock = new Object();
	
	public void init(S3Util s3Util) {
		this.s3Util = s3Util;
	}

	@Override
	public Integer call() throws Exception {
		int exit_code = 0;
		try {
			synchronized (lock) {
				while(true) {
					final File f = S3_Test_Multi.fetchFileList();
					if (f == null) {
						System.out.println("File Upload 처리 완료!!!");
						break;
					} else {
						System.out.println("File Upload Target [ "+f.getAbsolutePath()+" ]");
					}
					
					uploadExecute(f);
				}
				
			}
		} catch (Exception e) {
			e.printStackTrace();
			exit_code = 1;
		}

		return exit_code;
	}

	private void uploadExecute(final File f) {
		s3Util.uploadToS3UsingTM("build-tiger1/multi",f);
	}

}
