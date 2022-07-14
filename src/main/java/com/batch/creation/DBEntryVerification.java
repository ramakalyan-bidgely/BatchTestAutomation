package com.batch.creation;


import com.batch.utils.sql.batch.BatchDetails;
import com.batch.utils.sql.batch.BatchJDBCTemplate;
import com.google.gson.JsonObject;
import org.json.JSONObject;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.stereotype.Component;
import org.testng.Reporter;

import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.UUID;

/**
 * @author Rama Kalyan
 */
@Component
public class DBEntryVerification {


    public static ClassPathXmlApplicationContext context = new ClassPathXmlApplicationContext("classpath*:batch-dao.xml");


    public static BatchJDBCTemplate batchJDBCTemplate = (BatchJDBCTemplate) context.getBean("batchjdbcTemplate");

    public static Boolean validate(JsonObject manifestObjContent) {

        UUID batchId = UUID.fromString(manifestObjContent.get("batchId").getAsString());
        String batch_creation_type=manifestObjContent.get("batch_creation_type").getAsString();
        //Timestamp latest_modified_time=manifestObjContent.getAsTimestamp("");

        if (batchId.version() == 4) {
            List<BatchDetails> BatchIdEntry = batchJDBCTemplate.getBatch(String.valueOf(batchId));
            Reporter.log("Batch ID is valid and available : " + BatchIdEntry.size(), true);
            if (BatchIdEntry.get(0).getBatch_creation_type().equals(batch_creation_type)) {
                Reporter.log("Batch Creation Type matched", true);
            } else {
                Reporter.log("Mismatch in Batch creation type for this batch", true);
            }
            return (BatchIdEntry.size() > 0);
        } else {
            Reporter.log("BatchID is Invalid", true);
            return false;
        }
    }

    public static Timestamp getLatestBatchCreationTime(Integer pilot_id, String component) {
        Timestamp batch_creation_time = batchJDBCTemplate.getLatestBatchCreationTime(pilot_id, component);
        if (batch_creation_time != null) {
            Reporter.log("Latest Batch Creation Time: " + batch_creation_time, true);
        } else {
            Date now = new Date();
            Timestamp ts = new Timestamp(now.getTime());
            batch_creation_time = ts;
            Reporter.log(batch_creation_time + " : No batches found in the batch_details table with pilot = " + pilot_id + " and component = " + component + ", Considering it as a first run !", true);

        }
        return batch_creation_time;
    }


}
