package com.batch.creation;

import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.AmazonS3URI;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.batch.utils.S3FileTransferHandler;
import com.batch.utils.sql.batch.BatchJDBCTemplate;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.testng.Reporter;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.Timestamp;
import java.util.*;


/**
 * @author Rama Kalyan
 */
public class BatchCountValidator {
    static AmazonS3Client amazons3Client = new AmazonS3Client();


    public static JsonObject convertingToJsonObject(String jsonFilePath) {
        try {
            JsonObject json_object = new JsonParser().parse(jsonFilePath).getAsJsonObject();
            return json_object;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }


    }

    private static String displayTextInputStream(InputStream input) throws IOException {
        // Read the text input stream one line at a time and display each line.
        BufferedReader reader = new BufferedReader(new InputStreamReader(input));
        StringBuffer sb = new StringBuffer();
        String line;
        while ((line = reader.readLine()) != null) {
            sb.append(line);
        }
        return sb.toString();

    }

    public static List<String> getBatchManifestFileList(Integer pilotId, String component, String s3Bucket, String manifest_prefix, Timestamp batch_creation_time) {
        ListObjectsV2Request req = new ListObjectsV2Request().withBucketName(s3Bucket).withPrefix(manifest_prefix).withDelimiter("/");

        ListObjectsV2Result fileObjs = amazons3Client.listObjectsV2(req);
        List<S3ObjectSummary> summaries = fileObjs.getObjectSummaries();
        Reporter.log("Summaries count :  " + summaries.size(), true);
        List<String> manifestObjs = new ArrayList<String>();
        if (batch_creation_time != null) {
            batch_creation_time = Timestamp.from(batch_creation_time.toInstant().plusSeconds(2));
        }
        for (S3ObjectSummary summary : summaries) {
            if ((batch_creation_time != null && batch_creation_time.compareTo(summary.getLastModified()) <= 0)) {
                Reporter.log("batch Creation Time -> " + batch_creation_time + ",  Manifest Object -> " + summary.getKey(), true);
                Reporter.log("Manifest Object generated time :  " + summary.getLastModified(), true);
                Reporter.log(summary.getKey() + " : Considered", true);
                manifestObjs.add(summary.getKey());
            }
        }
        return manifestObjs;
    }

    public static List<String> getBatchManifestFiles(Integer pilotId, String component, String s3Bucket, String manifest_prefix) {
        ListObjectsV2Request req = new ListObjectsV2Request().withBucketName(s3Bucket).withPrefix(manifest_prefix).withDelimiter("/");
        ListObjectsV2Result fileObjs = amazons3Client.listObjectsV2(req);
        List<S3ObjectSummary> summaries = fileObjs.getObjectSummaries();
        List<String> manifestObjs = new ArrayList<String>();
        Reporter.log("In BatchCountValidator", true);
        for (S3ObjectSummary summary : summaries) {
            manifestObjs.add(summary.getKey());
        }
        return manifestObjs;
    }

    public static void getManifestFiles(Integer pilotId, String component, String s3Bucket, String manifest_prefix) {
        ListObjectsV2Request req = new ListObjectsV2Request().withBucketName(s3Bucket).withPrefix(manifest_prefix).withDelimiter("/");
        ListObjectsV2Result fileObjs = amazons3Client.listObjectsV2(req);
        List<S3ObjectSummary> summaries = fileObjs.getObjectSummaries();
        List<String> manifestObjs = new ArrayList<String>();

    }


    public static Integer getExpectedNoOfBatches(String s3Bucket, String prefix, Long dataSizeInBytes, Integer maxLookUpDays, Timestamp latest_modified_time, Timestamp batch_creation_time, long intervalInSec) {

        Integer expectedNumberOfBatches = 0;
        long tempAccumulatedSize = 0L;
        BatchJDBCTemplate batchJDBCTemplate = new BatchJDBCTemplate();

        ListObjectsV2Request ListObjreq = new ListObjectsV2Request().withBucketName(s3Bucket).withPrefix(prefix);
        ArrayList<S3ObjectSummary> summ = new ArrayList<>();
        ListObjectsV2Result objs = null;
        do {
            objs = amazons3Client.listObjectsV2(ListObjreq);
            summ.addAll(objs.getObjectSummaries());
            ListObjreq.setContinuationToken(objs.getNextContinuationToken());
        } while (objs.isTruncated());


        for (S3ObjectSummary summary : summ) {
            if (latest_modified_time.compareTo(summary.getLastModified()) < 0) {
                Reporter.log("Objects accumulated : ", true);
                Reporter.log(summary.getKey(), true);
                tempAccumulatedSize += summary.getSize();
                if (tempAccumulatedSize >= dataSizeInBytes) {
                    expectedNumberOfBatches++;
                    tempAccumulatedSize = 0;
                }
            }
        }
        if (expectedNumberOfBatches == 0) {
            Reporter.log("Configured Sized Data " + dataSizeInBytes + " hasn't been accumulated. Hence one TIME_BASED manifest batch may get generated !", true);
            expectedNumberOfBatches = 1;
        }

        return expectedNumberOfBatches;
    }

    public static Long getAccumulatedSize(Integer pilotId, String component, String s3Bucket, String prefix) {
        Map<String, Long> stats = new HashMap<>();
        long DataAccumulatedSize = 0L;
        BatchJDBCTemplate batchJDBCTemplate = new BatchJDBCTemplate();

        Timestamp latest_modified_time = batchJDBCTemplate.getLatestObjectDetails(pilotId, component);
        if (latest_modified_time == null) {
            Date now = new Date();
            Timestamp ts = new Timestamp(now.getTime());
            latest_modified_time = ts;
        }

        ListObjectsV2Request ListObjreq = new ListObjectsV2Request().withBucketName(s3Bucket);
        ArrayList<S3ObjectSummary> summ = new ArrayList<>();
        ListObjectsV2Result objs = null;
        do {
            objs = amazons3Client.listObjectsV2(ListObjreq);
            summ.addAll(objs.getObjectSummaries());
            ListObjreq.setContinuationToken(objs.getNextContinuationToken());
        } while (objs.isTruncated());


        for (S3ObjectSummary summary : summ) {
            if (latest_modified_time.compareTo(summary.getLastModified()) <= 0) {
                Reporter.log(String.valueOf(summary.getLastModified()), true);
                DataAccumulatedSize += summary.getSize();
            }
        }
        return DataAccumulatedSize;
    }


    public static long SizeOfObjects(String s3Bucket, JsonArray batchObjects) {
        long DataAccumulatedSize = 0;
        for (JsonElement arr : batchObjects) {
            long ObjectLength = amazons3Client.getObject(new GetObjectRequest(s3Bucket, arr.getAsString())).getObjectMetadata().getContentLength();
            DataAccumulatedSize += ObjectLength;
        }
        return DataAccumulatedSize;
    }


    public static long UploadAndAccumulate(String SRC, String DEST) {
        AmazonS3URI DEST_URI = new AmazonS3URI(DEST);
        long DataAccumulatedSize = S3FileTransferHandler.TransferFiles(DEST_URI, SRC);
        return DataAccumulatedSize;
    }

    //get Object list from batchManifestFile  and calculate size of those files
/*

    public static Long getAccumulatedSize(Integer pilotId, String s3Bucket, String prefix) {

        long DataAccumulatedSize = 0L;

        BatchJDBCTemplate batchJDBCTemplate = new BatchJDBCTemplate();
        List<BatchDetails> batch_details = batchJDBCTemplate.listBatches(pilotId);

        Reporter.log("Size:  ---------> " + batch_details.size(),true);

        for (BatchDetails str : batch_details) {
            Reporter.log(str.getLatest_modified_key(),true);
        }

        ListObjectsV2Request ListObjreq = new ListObjectsV2Request().withBucketName(s3Bucket).withStartAfter(prefix);

        ArrayList<S3ObjectSummary> summ = new ArrayList<>();

        ListObjectsV2Result objs = null;
        do {
            objs = amazons3Client.listObjectsV2(ListObjreq);
            Reporter.log(objs.getObjectSummaries() + "\n",true);
            summ.addAll(objs.getObjectSummaries());
            ListObjreq.setContinuationToken(objs.getNextContinuationToken());
        } while (objs.isTruncated());


        for (S3ObjectSummary summary : summ) {
            if (getLatest_modified_time().compareTo(summary.getLastModified()) >= 0) {
                Reporter.log(summary.getLastModified(),true);
                DataAccumulatedSize += summary.getSize();
            }
        }
        Reporter.log("DataAccumulated Size :  " + DataAccumulatedSize,true);
        return DataAccumulatedSize;
    }
*/

}


