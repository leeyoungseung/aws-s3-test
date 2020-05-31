package com.amazonaws;

import java.io.File;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import com.amazonaws.auth.AWSCredentials;
//import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
//import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.Bucket;
//import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.ListVersionsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.s3.model.S3VersionSummary;
import com.amazonaws.services.s3.model.VersionListing;
//import com.amazonaws.services.s3.transfer.MultipleFileUpload;
import com.amazonaws.services.s3.transfer.TransferManager;
//import com.amazonaws.services.s3.transfer.TransferManagerBuilder;
import com.amazonaws.services.s3.transfer.Upload;

public class S3Util {

	private AWSProp prop = null;
	private AmazonS3 s3 = null;
    
    private S3Util() {}
    public static S3Util getInstance() {
    	return LazyHolder.INSTANCE;
    }
    
    private static class LazyHolder {
    	private static final S3Util INSTANCE = new S3Util();
    }
    
    public void setAWSProp(AWSProp prop) {
    	this.prop = prop;
    }
	
    
	/**
	 * 인증 세팅
	 * 
	 * @return
	 */
	public void setAWSAuth() {
		System.out.println("setAWSAuth start");
		System.out.println();
		String access_key = prop.getAccessKey();
		String secret_key = prop.getSecretKey();
		System.out.println("AccessKey : "+access_key);
		System.out.println("SecretKey : "+secret_key);
		
		// set info of auth
		AWSCredentials credential = new BasicAWSCredentials(access_key, secret_key);
		// proxy setting

//		s3 = AmazonS3ClientBuilder.standard().withCredentials(new AWSStaticCredentialsProvider(credential))
//				.build();

		s3 = new AmazonS3Client(credential);
		
		System.out.println("setAWSAuth END");
	}

	/**
	 * 버킷 목록 조회
	 */
	public void getBucketList() {
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
	public Bucket getBucket(String bucket_name) {
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
	public void createBucket(String bucket_name) {
		System.out.println("createBucket START");
		Bucket bucket = null;
		//if (s3.doesBucketExistV2(bucket_name)) {
		if (s3.doesBucketExist(bucket_name)) {
			System.out.format("Bucket %s already exists.\n", bucket_name);
			bucket = getBucket(bucket_name);
		} else {
			try {
				bucket = s3.createBucket(bucket_name);
				System.out.println(bucket.getName());
			} catch (AmazonS3Exception e) {
				System.err.println(e.getMessage());
			}
		}
		System.out.println("createBucket END");
	}

	/**
	 * 지정한 버킷의 삭제
	 * 
	 * @param bucket_name
	 */
	public void deleteBucket(String bucket_name) {
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
			System.err.println(e.getMessage());
			System.exit(1);
		}

		System.out.println("deleteBucket END");
	}
	
	
	/**
	 * TransferManager를 사용한 단일 파일업로드 (2)
	 * 
	 * @param bucketName
	 * @param targetFile
	 */
	public void uploadToS3UsingTM(String bucketName, File targetFile) {
		System.out.println("uploadToS3UsingTM(2) START");

		File f = targetFile;

//		TransferManager xfer_mgr = TransferManagerBuilder.standard()
//				.withS3Client(s3)
//				.build();
		
		TransferManager xfer_mgr = new TransferManager(s3);
		
		try {
			Upload xfer = xfer_mgr.upload(bucketName, f.getName(), f);

            xfer.waitForCompletion();
		} catch (AmazonServiceException e) {
			System.err.println(e.getMessage());
			System.exit(2);
		} catch (AmazonClientException | InterruptedException e) {
			System.err.println(e.getMessage());
			System.exit(3);
		} finally {
			xfer_mgr.shutdownNow(false);
		}
		

		System.out.println("uploadToS3UsingTM(2) END");
	}
	
	/**
	 * 파일 목록 가져오기
	 * 
	 * @param bucket_name
	 */
	public void getFileList(String bucket_name) {
		System.out.println("getFileList START");
		System.out.format("Objects in S3 bucket %s:\n", bucket_name);
		ObjectListing result = s3.listObjects(bucket_name);
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
	public void deleteOnS3(String bucketName, String s3FilePath) {
		System.out.println("deleteOnS3 START");

		s3.deleteObject(bucketName, s3FilePath);

		System.out.println("deleteOnS3 START");
	}
}
