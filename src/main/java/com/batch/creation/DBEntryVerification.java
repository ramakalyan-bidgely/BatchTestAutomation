package com.batch.creation;


import com.batch.utils.sql.batch.BatchDetails;
import com.batch.utils.sql.batch.BatchJDBCTemplate;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.stereotype.Component;
import org.testng.Reporter;

import java.sql.Timestamp;
import java.util.List;
import java.util.UUID;

/**
 * @author Rama Kalyan
 */
@Component
public class DBEntryVerification {


    public static ClassPathXmlApplicationContext context = new ClassPathXmlApplicationContext("classpath*:batch-dao.xml");


    public static BatchJDBCTemplate batchJDBCTemplate = (BatchJDBCTemplate) context.getBean("batchjdbcTemplate");

    public static Boolean validate(UUID batchId) {
        if (batchId.version() == 4) {
            List<BatchDetails> BatchIdEntry = batchJDBCTemplate.getBatch(String.valueOf(batchId));
            System.out.println("Batch ID is valid and available : " + BatchIdEntry.size());
            return (BatchIdEntry.size() > 0);
        } else {
            Reporter.log("BatchID is Invalid");
            return false;
        }
    }

    public static Timestamp getLatestBatchCreationTime(Integer pilot_id, String component) {
        Timestamp batch_creation_time = batchJDBCTemplate.getLatestBatchCreationTime(pilot_id, component);
        if (!batch_creation_time.equals(null)) {
            System.out.println("Latest Batch Creation Time: " + batch_creation_time);
        } else {
            System.out.println("No batches found in the batch_details table with pilot = " + pilot_id + " and component = " + component);
        }
        return batch_creation_time;
    }



  /*
    Test_CaseID		  :	TC_BC_07
    Priority		    :	P0
    Area				    :	Batch Creation
    TestCaseName		:	Batch_DBEntry_Verification
    TestCaseSummary	:	Verify unique batch id generated in the manifest file followed by its entry in the database table (entry of that batch with status CREATED as default value)
    Steps			      :	1. Read/Parse successfully generated manifest file and get batchId value from it
                      2. check the uuid is valid or not  (ex : version 4)
                      3. Check the batch id entry in the db tables batch_details table
    ExpectedResult	:	A valid uuid (version 4) must be generated in batch manifest file with its entry in the mysql db tables batch_run_history, batchGlobalStatus without failure in the updation of db after generation of manifest file

   */




  /*val logger: Logger = LoggerFactory.getLogger(getClass.getSimpleName)

  def validate(batchId: UUID): Unit = {

    if (batchId.version() == 4) {
      Reporter.log("Batch UUID is valid with version 4")

      val QryStr = s"select status from batch.batchGlobalStatus where batchId='$batchId'"
      //Check Status of batch for batchId
      val Batch_Status = BatchInfoDB.getBatchStatus(QryStr, batchId.toString)
      if (Batch_Status == "CREATED") {
        Reporter.log("Batch created successfully for batchId -> {} - Test case Passed !", batchId)
      } else {
        Reporter.log("Batch entry is missing in the database for batchId -> {} - Test case Failed !", batchId)
      }
    } else {
      logger.error("Batch UUID is not a valid UUID - Test case Failed !")
    }
  }*/
}
