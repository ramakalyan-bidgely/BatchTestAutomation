package com.batch.utils;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.SdkClientException;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.AmazonS3URI;
import com.amazonaws.services.s3.model.CopyObjectRequest;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.s3.transfer.Transfer;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.TransferManagerBuilder;
import com.amazonaws.services.s3.transfer.TransferProgress;
import org.testng.Reporter;

import java.io.File;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static com.batch.utils.sql.batch.BatchDetails.getLatest_modified_time;

/**
 * @author Rama Kalyan
 */


public class S3FileTransferHandler {

    static AmazonS3Client amazonS3Client = new AmazonS3Client();
    List<String> ListOfEntities = Arrays.asList("ListOfEnrollmentFiles", "ListOfMeterFiles", "ListOfRAWDataFiles", "ListOfInvoiceFiles");

    public static long TransProgress(Transfer trans) {
        System.out.println(trans.getDescription());
        TransferProgress progress = trans.getProgress();
        long totalBytes = progress.getTotalBytesToTransfer();
        System.out.println("Total Bytes to be transferred: " + totalBytes);

        printProgressBar(0.0);
        do {
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                return 0;
            }
            long BytesTransferred = progress.getBytesTransferred();
            double pct = progress.getPercentTransferred();
            eraseProgressBar();
            printProgressBar(pct);
        } while (trans.isDone() == false);
        Transfer.TransferState transSt = trans.getState();
        System.out.println("Transfer State : " + transSt);
        return totalBytes;
    }

    public static void printProgressBar(double pct) {
        // if bar_size changes, then change erase_bar (in eraseProgressBar) to
        // match.
        final int bar_size = 40;
        final String empty_bar = "                                        ";
        final String filled_bar = "########################################";
        int amt_full = (int) (bar_size * (pct / 100.0));
        System.out.format("  [%s%s] %.2f ", filled_bar.substring(0, amt_full), empty_bar.substring(0, bar_size - amt_full), pct);
    }

    // erases the progress bar.
    public static void eraseProgressBar() {
        // erase_bar is bar_size (from printProgressBar) + 4 chars. + added Percentage also
        final String erase_bar = "\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b";
        System.out.format(erase_bar);
    }

    public static List<File> ListofFiles(String dir) {
        File dr = new File(dir);
        List<File> paths = null;
        if (dr.exists() && dr.isDirectory()) {
            paths = Arrays.asList(dr.listFiles());
        } else {
            System.out.println("Directory not found");
        }
        return paths;
    }

    public static String[] listFiles(String path) {
        //create a string[] pathnames call the function and store it in pathnames loop through it
        String[] pathnames;
        File f = new File(path);
        pathnames = f.list();
        return pathnames;
    }

    public static long TransferFiles(AmazonS3URI DEST_URI, String Dir) {

        long DataAccumulatedSize = 0;
        TransferManager tmClient = TransferManagerBuilder.standard().withS3Client(amazonS3Client).build();
        Transfer myUpload = tmClient.uploadFileList(DEST_URI.getBucket(), DEST_URI.getKey(), new File(Dir), ListofFiles(Dir));
        if (myUpload.isDone() == false) {
            System.out.println("Transfer: " + myUpload.getDescription());
            System.out.println("  - State: " + myUpload.getState());
            //System.out.println("  - Progress: " + myUpload.getProgress().getBytesTransferred());
        }

        // Transfers also allow you to set a <code>ProgressListener</code> to receive
        // asynchronous notifications about your transfer's progress.
      /*  myUpload.addProgressListener((ProgressListener) event -> {
            System.out.println("Progress In Bytes : ->" + event.getBytesTransferred());
        });*/

        // Or you can block the current thread and wait for your transfer to
        // to complete. If the transfer fails, this method will throw an
        // AmazonClientException or AmazonServiceException detailing the reason.

        try {
            DataAccumulatedSize = TransProgress(myUpload);
            myUpload.waitForCompletion();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        // After the upload is complete, call shutdownNow to release the resources.
        tmClient.shutdownNow();


      /*  AmazonS3Client amazonS3Client = new AmazonS3Client();

        String Targetkey = DEST_URI.getKey().replace("s3://", "").replace(DEST_URI.getBucket(), "");
        for (File fl : ListofFiles(Dir)) {
            System.out.println("File Transferring : " + fl.getName());
            amazonS3Client.putObject(DEST_URI.getBucket(), Targetkey + "/" + fl.getName(), fl);
        }
        System.out.println("Files have been uploaded successfully");*/
        return DataAccumulatedSize;

    }

    public static ArrayList<String> GetObjectKeys(AmazonS3URI SRC_URI) {
        com.amazonaws.services.s3.model.ListObjectsV2Request ListObjreq = new ListObjectsV2Request().withBucketName(SRC_URI.getBucket()).withPrefix(SRC_URI.getKey());
        ArrayList<S3ObjectSummary> summ = new ArrayList<>();
        ArrayList<String> keys = new ArrayList<>();
        ListObjectsV2Result objs = null;
        do {
            objs = amazonS3Client.listObjectsV2(ListObjreq);
            summ.addAll(objs.getObjectSummaries());
            ListObjreq.setContinuationToken(objs.getNextContinuationToken());
        } while (objs.isTruncated());


        for (S3ObjectSummary summary : summ) {
            keys.add(summary.getKey());
        }
        ArrayList<String> objects = new ArrayList<>();
        for (int i = 1; i < keys.size(); i++) {
            String[] values = keys.get(i).split("/");
            objects.add(values[values.length - 1]);
        }
        return keys;

    }


    public static long S3toS3TransferFiles(AmazonS3URI DEST_URI, AmazonS3URI SRC_URI) {
        long DataAccumulatedSize = 0;
        try {
            //final AmazonS3 amazons3Client = AmazonS3ClientBuilder.standard().withRegion(Regions.DEFAULT_REGION).build();
            AmazonS3Client amazonS3Client = new AmazonS3Client();

            ListObjectsV2Request ListObjreq = new ListObjectsV2Request().withBucketName(SRC_URI.getBucket()).withPrefix(SRC_URI.getKey());
            ArrayList<S3ObjectSummary> summ = new ArrayList<>();
            ListObjectsV2Result objs = null;
            do {
                objs = amazonS3Client.listObjectsV2(ListObjreq);
                summ.addAll(objs.getObjectSummaries());

                ListObjreq.setContinuationToken(objs.getNextContinuationToken());
            } while (objs.isTruncated());

            for (S3ObjectSummary summary : summ) {
                List<String> ObjName = Arrays.asList(summary.getKey().split("/"));
                CopyObjectRequest copyObjRequest = new CopyObjectRequest(summary.getBucketName(), summary.getKey(), DEST_URI.getBucket(), DEST_URI.getKey() + "/" + ObjName.get(ObjName.size() - 1));
                amazonS3Client.copyObject(copyObjRequest);
                Reporter.log(ObjName + " transferred", true);
                DataAccumulatedSize += summary.getSize();
            }

        } catch (AmazonServiceException e) {
            e.printStackTrace();
        } catch (SdkClientException e) {
            e.printStackTrace();
        }
        return DataAccumulatedSize;
    }
}

