package com.batch.creation;


import com.batch.utils.*;
import com.batch.utils.sql.batch.BatchDetails;
import com.batch.utils.sql.batch.BatchJDBCTemplate;
import com.google.gson.JsonObject;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.stereotype.Component;
import org.testng.Reporter;

import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.ZonedDateTime;
import java.util.*;

import static com.batch.api.common.Constants.InputConfigConstants.*;
import static com.batch.utils.VariableCollections.RunType;


/**
 * @author Rama Kalyan
 */
@Component
public class DBEntryVerification {


    public static ClassPathXmlApplicationContext context = new ClassPathXmlApplicationContext("classpath*:batch-dao.xml");


    public static BatchJDBCTemplate batchJDBCTemplate = (BatchJDBCTemplate) context.getBean("batchjdbcTemplate");

    public static Boolean validate(JsonObject manifestObjContent) {

        UUID batchId = UUID.fromString(manifestObjContent.get(BATCH_ID).getAsString());
        Integer pilotId = manifestObjContent.get(PILOT_ID).getAsInt();
        String component = manifestObjContent.get(COMPONENT).getAsString();
        String batch_creation_type = manifestObjContent.get(BATCH_CREATION_TYPE).getAsString();
        Timestamp latest_modified_time = Timestamp.valueOf(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date(manifestObjContent.get(LATEST_OBJECT_MODIFIED_TIME).getAsLong())));
        Timestamp batch_creation_time = Timestamp.valueOf(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date(manifestObjContent.get(BATCH_CREATION_TIME).getAsLong())));
        String latest_object_key = manifestObjContent.get(LATEST_OBJECT_KEY).getAsString();

        if (batchId.version() == 4) {

            List<BatchDetails> BatchIdEntry = batchJDBCTemplate.getBatch(String.valueOf(batchId));
            Reporter.log("Batch ID is valid and available ->" + batchId, true);

            if (pilotId == BatchIdEntry.get(0).getPilot_id()) {
                Reporter.log("Pilot ID is matched", true);
            } else {
                Reporter.log("Mismatch in Pilot ID", true);
                return false;
            }
            if (component.equals(BatchIdEntry.get(0).getComponent())) {
                Reporter.log("Component is matched", true);
            } else {
                Reporter.log("Mismatch in Component ID", true);
                return false;
            }


            Reporter.log("Batch creation time in manifest ->" + batch_creation_time, true);
            Reporter.log("Batch creation time in db->" + Timestamp.valueOf(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(BatchIdEntry.get(0).getBatch_creation_time())), true);

            if (batch_creation_time.compareTo(Timestamp.valueOf(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(BatchIdEntry.get(0).getBatch_creation_time()))) == 0) {
                Reporter.log("Batch creation time matched", true);
            } else {
                Reporter.log("Mismatch in Batch creation time", true);
                return false;
            }


            if (latest_object_key.equals(BatchIdEntry.get(0).getLatest_modified_key())) {
                Reporter.log("Latest Modified key is valid", true);
            } else {
                Reporter.log("Latest Modified key is Invalid", true);
                return false;
            }

            if (BatchIdEntry.get(0).getBatch_creation_type().equals(batch_creation_type)) {
                Reporter.log("Batch Creation Type matched", true);
            } else {
                Reporter.log("Mismatch in Batch creation type for this batch", true);
                return false;
            }

            if (latest_modified_time.compareTo(BatchIdEntry.get(0).getlatest_modified_time()) == 0) {
                Reporter.log("Latest Modified time matched", true);
            } else {
                Reporter.log("Mismatch in Latest Modified time", true);
                return false;
            }


            return (BatchIdEntry.size() > 0);
        } else {
            Reporter.log("BatchID is Invalid", true);
            return false;
        }


    }


    public static Timestamp getLatestModifiedTime(Integer pilot_id, String component) {
        List<Map<String, Object>> latestObjectDetails = batchJDBCTemplate.getLatestObjectDetails(pilot_id, component);

        Timestamp latest_modified_time = (Timestamp) (latestObjectDetails.size() > 0 ? latestObjectDetails.get(0).get(LATEST_MODIFIED_TIME) : new Timestamp(new Date().getTime()));


        if (latest_modified_time != null) {
            Reporter.log("Latest Object Modified Time: " + latest_modified_time, true);
        } else {
            Date now = new Date();
            Timestamp ts = new Timestamp(now.getTime());
            latest_modified_time = ts;
            Reporter.log(latest_modified_time + " : No batches found in the batch_details table with pilot = " + pilot_id + " and component = " + component + ", Considering it as a first run !", true);
        }
        return latest_modified_time;
    }

    public static Timestamp getLatestBatchCreationTime(Integer pilot_id, String component) {
        Timestamp batch_creation_time = batchJDBCTemplate.getLatestBatchCreationTime(pilot_id, component);
        if (batch_creation_time != null) {
            Reporter.log("Latest Batch Creation Time: " + batch_creation_time, true);
            VariableCollections.map.put("RunType", "SubseqRun");
        } else {
            VariableCollections.map.put("RunType", "InitRun");
            Date now = new Date();
            Timestamp ts = new Timestamp(now.getTime());
            batch_creation_time = ts;
            Reporter.log(batch_creation_time + " : No batches found in the batch_details table with pilot = " + pilot_id + " and component = " + component + ", Considering it as a first run !", true);
        }
        return batch_creation_time;
    }


}
