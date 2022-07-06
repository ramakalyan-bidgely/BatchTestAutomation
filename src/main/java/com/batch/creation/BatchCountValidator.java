package com.batch.creation;

import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.AmazonS3URI;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.batch.utils.S3FileTransferHandler;
import com.batch.utils.sql.batch.BatchDetails;
import com.batch.utils.sql.batch.BatchJDBCTemplate;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import static com.batch.utils.sql.batch.BatchDetails.getLatest_modified_time;

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
        List<String> manifestObjs = new ArrayList<String>();
        System.out.println("In BatchCountValidator");
        for (S3ObjectSummary summary : summaries) {
            if (batch_creation_time.compareTo(summary.getLastModified()) >= 0) {
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
        System.out.println("In BatchCountValidator");
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
        System.out.println("In gettingManifestFiles");
    }

    public static Long getAccumulatedSize(Integer pilotId, String s3Bucket, String prefix) {

        long DataAccumulatedSize = 0L;

        BatchJDBCTemplate batchJDBCTemplate = new BatchJDBCTemplate();
        List<BatchDetails> batch_details = batchJDBCTemplate.listBatches(pilotId);

        System.out.println("Size:  ---------> " + batch_details.size());

        for (BatchDetails str : batch_details) {
            System.out.println(str.getLatest_modified_key());
        }

        ListObjectsV2Request ListObjreq = new ListObjectsV2Request().withBucketName(s3Bucket).withStartAfter(prefix);

        ArrayList<S3ObjectSummary> summ = new ArrayList<>();

        ListObjectsV2Result objs = null;
        do {
            objs = amazons3Client.listObjectsV2(ListObjreq);
            System.out.println(objs.getObjectSummaries() + "\n");
            summ.addAll(objs.getObjectSummaries());
            ListObjreq.setContinuationToken(objs.getNextContinuationToken());
        } while (objs.isTruncated());


        for (S3ObjectSummary summary : summ) {
            if (getLatest_modified_time().compareTo(summary.getLastModified()) >= 0) {
                System.out.println(summary.getLastModified());
                DataAccumulatedSize += summary.getSize();
            }
        }
        System.out.println("DataAccumulated Size :  " + DataAccumulatedSize);
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

        System.out.println("Size:  ---------> " + batch_details.size());

        for (BatchDetails str : batch_details) {
            System.out.println(str.getLatest_modified_key());
        }

        ListObjectsV2Request ListObjreq = new ListObjectsV2Request().withBucketName(s3Bucket).withStartAfter(prefix);

        ArrayList<S3ObjectSummary> summ = new ArrayList<>();

        ListObjectsV2Result objs = null;
        do {
            objs = amazons3Client.listObjectsV2(ListObjreq);
            System.out.println(objs.getObjectSummaries() + "\n");
            summ.addAll(objs.getObjectSummaries());
            ListObjreq.setContinuationToken(objs.getNextContinuationToken());
        } while (objs.isTruncated());


        for (S3ObjectSummary summary : summ) {
            if (getLatest_modified_time().compareTo(summary.getLastModified()) >= 0) {
                System.out.println(summary.getLastModified());
                DataAccumulatedSize += summary.getSize();
            }
        }
        System.out.println("DataAccumulated Size :  " + DataAccumulatedSize);
        return DataAccumulatedSize;
    }
*/

}


