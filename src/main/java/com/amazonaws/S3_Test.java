package com.amazonaws;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.CannedAccessControlList;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.ListVersionsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.s3.model.S3VersionSummary;
import com.amazonaws.services.s3.model.VersionListing;
import com.amazonaws.services.s3.transfer.MultipleFileUpload;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.TransferManagerBuilder;
import com.amazonaws.services.s3.transfer.Upload;

public class S3_Test {

	static AWSProp prop = new AWSProp();
	static AmazonS3 s3 = null;
    static TransferManager xfer_mgr = null;
	
	public static void main(String[] args) {
		
		// 1. AccessKey와 SecretAccesskey로 AmazonS3 객체 생성
		System.out.println("=============== (1) START ===============");
		System.out.println("");
		
		setAWSAuth();
		
		System.out.println("");
		System.out.println("=============== (1) END =================");
		
		
		// 2. 버킷 생성하기
		System.out.println("=============== (2) START ===============");
		System.out.println("");
		
		createBucket(prop.getBucketName1());
		createBucket(prop.getBucketName2());
		
		System.out.println("");
		System.out.println("=============== (2) END =================");
		
		
		// 3. 버킷 & 버킷내 파일 조회하기
		System.out.println("=============== (3) START ===============");
		System.out.println("");
		
		getBucketList();
		getFileList(prop.getBucketName1());
		getFileList(prop.getBucketName2());

		System.out.println("");
		System.out.println("=============== (3) END =================");
		
		
		// 4. 파일 업로드하기 (AmazonS3 객체로 업로드하기)
		System.out.println("=============== (4) START ===============");
		System.out.println("");
		
		uploadToS3(prop.getBucketName1(), "test1.txt", "C:\\s3_test\\test1.txt");
		uploadToS3(prop.getBucketName1() + "/s3_test", "test1.txt", "C:\\s3_test\\test1.txt");
		getFileList(prop.getBucketName1());
		
		System.out.println("");
		System.out.println("=============== (4) END =================");

		
		// 5. 파일 삭제하기
		System.out.println("=============== (5) START ===============");
		System.out.println("");
		
		deleteOnS3(prop.getBucketName1(), "test1.txt");
		getFileList(prop.getBucketName1());
		deleteOnS3(prop.getBucketName1(), "s3_test/test1.txt");
		getFileList(prop.getBucketName1());
		
		System.out.println("");
		System.out.println("=============== (5) END =================");
		
		
		// 6. 파일 업로드하기 (TransferManager를 사용한 단일 파일업로드)
		System.out.println("=============== (6) START ===============");
		System.out.println("");
		
		uploadToS3UsingTM(prop.getBucketName1(), "C:\\s3_test\\test2.txt", true);
		getFileList(prop.getBucketName1());
		uploadToS3UsingTM(prop.getBucketName1() + "/s3_test2", "C:\\s3_test\\test2.txt", false);
		getFileList(prop.getBucketName1());
		
		System.out.println("");
		System.out.println("=============== (6) END =================");
		
		
		// 7. 다수파일 업로드하기 (TransferManager를 사용한 다수 파일업로드)
		System.out.println("=============== (7) START ===============");
		System.out.println("");
		
		String[] file_paths = new String[] { "C:\\s3_test\\dummy1\\test5-1.txt",
				"C:\\s3_test\\dummy1\\test5-2.txt",
				"C:\\s3_test\\dummy1\\test5-3.txt" };
		uploadToS3UsingTMFileList(prop.getBucketName2(), "s3_test_list", "C:\\s3_test\\dummy1", file_paths, true);
		getFileList(prop.getBucketName2());
		uploadToS3UsingTMFileList(prop.getBucketName2() + "/s3_test_list2", "", "C:\\s3_test\\dummy1", file_paths,
				true);
		getFileList(prop.getBucketName2());
		
		System.out.println("");
		System.out.println("=============== (7) END =================");
		
		
		// 8. 디렉토리 업로드<동기화> (TransferManager를 사용한 디렉토리 동기화)
		System.out.println("=============== (8) START ===============");
		System.out.println("");
		
		uploadToUsingTMDir(prop.getBucketName2(), "dummy2-1", "C:\\s3_test\\dummy2", false);
		getFileList(prop.getBucketName2());
		uploadToUsingTMDir(prop.getBucketName2() + "/dummy2-2", "", "C:\\s3_test\\dummy2", true);
		getFileList(prop.getBucketName2());
		
		System.out.println("");
		System.out.println("=============== (8) END =================");
		
		
		// 9. 버킷삭제하기
		System.out.println("=============== (9) START ===============");
		System.out.println("");
		
		deleteBucket(prop.getBucketName1());
		deleteBucket(prop.getBucketName2());
		getBucketList();

		System.out.println("");
		System.out.println("=============== (9) END =================");
	}

	/**
	 * 인증 세팅
	 * 
	 * @return
	 */
	private static void setAWSAuth() {
		System.out.println("setAWSAuth start");
		System.out.println();
		String access_key = prop.getAccessKey();
		String secret_key = prop.getSecretKey();
		System.out.println("AccessKey : "+access_key);
		System.out.println("SecretKey : "+secret_key);
		
		// set info of auth
		AWSCredentials credential = new BasicAWSCredentials(access_key, secret_key);

		// proxy setting
//		ClientConfiguration conf = new ClientConfiguration();
//		conf.setProxyHost("");
//		conf.setProxyPassword("");

		// endpoint setting ("$endpoint", "$region")
//		EndpointConfiguration endpointConfiguration = new EndpointConfiguration("", "");

		s3 = AmazonS3ClientBuilder.standard().withCredentials(new AWSStaticCredentialsProvider(credential))
				// .withClientConfiguration(conf)
				// .withEndpointConfiguration(endpointConfiguration)
				.build();
		
		xfer_mgr = TransferManagerBuilder.standard()
				.withS3Client(s3)
				.build();

		System.out.println("setAWSAuth END");
	}

	/**
	 * 버킷 목록 조회
	 */
	public static void getBucketList() {
		System.out.println("========== Bucket List Start ==========");
		List<Bucket> buckets = s3.listBuckets();
		for (Bucket b : buckets) {
			System.out.println(b.getName());
		}
		System.out.println("========== Bucket List End   ==========");
	}

	/**
	 * 버킷 목록 조회 후 bucket_name의 값에 해당하는 버킷이 있는지 확인
	 * 
	 * @param bucket_name
	 * @return
	 */
	public static Bucket getBucket(String bucket_name) {
		System.out.println("getBucket Start");

		System.out.println("I'm going to find Bucket : " + bucket_name);
		Bucket named_bucket = null;
		List<Bucket> buckets = s3.listBuckets();

		System.out.println("========== Bucket List Start ==========");
		for (Bucket b : buckets) {
			System.out.println(b.getName());
		}
		System.out.println("========== Bucket List End   ==========");

		for (Bucket b : buckets) {
			if (b.getName().equals(bucket_name)) {
				named_bucket = b;
				System.out.println("found Bucket Name : " + named_bucket.getName());
			}
		}

		System.out.println("getBucket END");
		return named_bucket;
	}

	/**
	 * 버킷을 생성한다.
	 * 
	 * @param bucketName : 생성할 버킷명
	 */
	private static void createBucket(String bucket_name) {
		System.out.println("createBucket START");
		Bucket bucket = null;
		if (s3.doesBucketExistV2(bucket_name)) {
			System.out.format("Bucket %s already exists.\n", bucket_name);
			bucket = getBucket(bucket_name);
		} else {
			try {
				bucket = s3.createBucket(bucket_name);
				System.out.println(bucket.getName());
			} catch (AmazonS3Exception e) {
				System.err.println(e.getErrorMessage());
			}
		}
		System.out.println("createBucket END");
	}

	/**
	 * 지정한 버킷의 삭제
	 * 
	 * @param bucket_name
	 */
	private static void deleteBucket(String bucket_name) {
		System.out.println("deleteBucket START");
		try {
			System.out.println(" - removing objects from bucket");
			ObjectListing object_listing = s3.listObjects(bucket_name);

			while (true) {
				for (Iterator<?> iterator = object_listing.getObjectSummaries().iterator(); iterator.hasNext();) {
					S3ObjectSummary summary = (S3ObjectSummary) iterator.next();
					s3.deleteObject(bucket_name, summary.getKey());
				}

				// more object_listing to retrieve?
				if (object_listing.isTruncated()) {
					object_listing = s3.listNextBatchOfObjects(object_listing);
				} else {
					break;
				}
			}

			System.out.println(" - removing versions from bucket");
			VersionListing version_listing = s3.listVersions(new ListVersionsRequest().withBucketName(bucket_name));

			while (true) {
				for (Iterator<?> iterator = version_listing.getVersionSummaries().iterator(); iterator.hasNext();) {
					S3VersionSummary vs = (S3VersionSummary) iterator.next();
					s3.deleteVersion(bucket_name, vs.getKey(), vs.getVersionId());
				}

				if (version_listing.isTruncated()) {
					version_listing = s3.listNextBatchOfVersions(version_listing);
				} else {
					break;
				}
			}

			System.out.println(" OK, bucket ready to delete!");
			s3.deleteBucket(bucket_name);
		} catch (AmazonServiceException e) {
			System.err.println(e.getErrorMessage());
			System.exit(1);
		}

		System.out.println("deleteBucket END");
	}

	/**
	 * 파일 업로드
	 * 
	 * @param bucketName
	 * @param s3FilePath
	 * @param localFilePath
	 */
	private static void uploadToS3(String bucketName, String s3FilePath, String localFilePath) {
		System.out.println("uploadToS3 START");

		File file = null;
		FileInputStream fis = null;
		ObjectMetadata om = null;

		try {
			file = new File(localFilePath);
			System.out.println("getName : [" + file.getName() + "]");
			System.out.println("getParent : [" + file.getParent() + "]");
			System.out.println("getAbsolutePath : [" + file.getAbsolutePath() + "]");
			System.out.println("getCanonicalPath : [" + file.getCanonicalPath() + "]");

			fis = new FileInputStream(file);
			om = new ObjectMetadata();
			om.setContentLength(file.length());

			// PutObjectRequest put = new PutObjectRequest(bucketName, file.getName(),
			// file);
			final PutObjectRequest req = new PutObjectRequest(bucketName, file.getName(), fis, om);

			// set file permission
			req.setCannedAcl(CannedAccessControlList.Private);

			// upload
			s3.putObject(req);

			fis.close();

		} catch (IOException ioe) {
			ioe.printStackTrace();
		} catch (AmazonServiceException ase) {
			System.out.println("Caught an AmazonServiceException, which means your request made it "
					+ "to Amazon S3, but was rejected with an error response for some reason.");
			System.out.println("Error Message:    " + ase.getMessage());
			System.out.println("HTTP Status Code: " + ase.getStatusCode());
			System.out.println("AWS Error Code:   " + ase.getErrorCode());
			System.out.println("Error Type:       " + ase.getErrorType());
			System.out.println("Request ID:       " + ase.getRequestId());
		} catch (AmazonClientException ace) {
			System.out.println("Caught an AmazonClientException, which means the client encountered "
					+ "a serious internal problem while trying to communicate with S3, "
					+ "such as not being able to access the network.");
			System.out.println("Error Message: " + ace.getMessage());
		}
		System.out.println("uploadToS3 END");
	}

	/**
	 * TransferManager를 사용한 단일 파일업로드
	 * 
	 * @param file_path
	 * @param bucket_name
	 * @param key_prefix
	 * @param pause
	 */
	private static void uploadToS3UsingTM(String bucketName, String s3FilePath, boolean pause) {
		System.out.println("uploadToS3UsingTM START");

		File f = new File(s3FilePath);

		xfer_mgr = TransferManagerBuilder.standard().build();
		try {
			Upload xfer = xfer_mgr.upload(bucketName, f.getName(), f);
			// loop with Transfer.isDone()
			XferMgrProgress.showTransferProgress(xfer);
			// or block with Transfer.waitForCompletion()
			XferMgrProgress.waitForCompletion(xfer);
		} catch (AmazonServiceException e) {
			System.err.println(e.getErrorMessage());
			System.exit(1);
		}
		xfer_mgr.shutdownNow();

		System.out.println("uploadToS3UsingTM END");
	}

	/**
	 * 다수 파일 업로드
	 * 
	 * @param bucket_name
	 * @param key_prefix
	 * @param parentsDir
	 * @param file_paths
	 * @param pause
	 */
	private static void uploadToS3UsingTMFileList(String bucket_name, String key_prefix, String parentsDir,
			String[] file_paths, boolean pause) {
		System.out.println("uploadToS3UsingTMFileList START");

		ArrayList<File> files = new ArrayList<File>();
		for (String path : file_paths) {
			files.add(new File(path));
		}

		xfer_mgr = TransferManagerBuilder.standard().build();
		try {
			MultipleFileUpload xfer = xfer_mgr.uploadFileList(bucket_name, key_prefix, new File(parentsDir), files);
			// loop with Transfer.isDone()
			XferMgrProgress.showTransferProgress(xfer);
			// or block with Transfer.waitForCompletion()
			XferMgrProgress.waitForCompletion(xfer);
		} catch (AmazonServiceException e) {
			System.err.println(e.getErrorMessage());
			System.exit(1);
		}
		xfer_mgr.shutdownNow();
		System.out.println("uploadToS3UsingTMFileList END");
	}

	/**
	 * 디렉토리 업로드 (동기화)
	 * 
	 * @param bucket_name
	 * @param dir_path    업로드할 곳의 경로
	 * @param key_prefix  공통 경로
	 * @param recursive   하위 디렉토리 포함 true , 하위디렉토리 비포함 false
	 */
	private static void uploadToUsingTMDir(String bucket_name, String key_prefix, String dir_path, boolean recursive) {
		System.out.println("uploadToUsingTMDir START");
		xfer_mgr = TransferManagerBuilder.standard().build();

		try {
			MultipleFileUpload xfer = xfer_mgr.uploadDirectory(bucket_name, key_prefix, new File(dir_path), recursive);
			// loop with Transfer.isDone()
			XferMgrProgress.showTransferProgress(xfer);
			// or block with Transfer.waitForCompletion()
			XferMgrProgress.waitForCompletion(xfer);
		} catch (AmazonServiceException e) {
			System.err.println(e.getErrorMessage());
			System.exit(1);
		}
		xfer_mgr.shutdownNow();
		System.out.println("uploadToUsingTMDir END");
	}

	/**
	 * 파일 목록 가져오기
	 * 
	 * @param bucket_name
	 */
	private static void getFileList(String bucket_name) {
		System.out.println("getFileList START");
		System.out.format("Objects in S3 bucket %s:\n", bucket_name);
		ListObjectsV2Result result = s3.listObjectsV2(bucket_name);
		List<S3ObjectSummary> objects = result.getObjectSummaries();
		for (S3ObjectSummary os : objects) {
			System.out.println("* " + os.getKey());
		}
		System.out.println("getFileList END");
	}

	/**
	 * 파일 삭제
	 * 
	 * @param bucketName
	 * @param s3FilePath
	 */
	private static void deleteOnS3(String bucketName, String s3FilePath) {
		System.out.println("deleteOnS3 START");

		s3.deleteObject(bucketName, s3FilePath);

		System.out.println("deleteOnS3 START");
	}

}
