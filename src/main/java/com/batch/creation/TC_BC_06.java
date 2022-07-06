package com.batch.creation;

import com.batch.utils.ManifestFileParser;
import com.batch.utils.ManifestResponse;
import com.batch.utils.VariableCollections;
import com.google.gson.JsonObject;
import org.testng.Reporter;

import java.util.List;
import java.util.UUID;

/**
 * @author Rama Kalyan
 */
public class TC_BC_06 {

  /*
      Test_CaseID		    :	TC_BC_06
      Priority		      :	P0
      Area				      :	Batch Creation
      TestCaseName		  :	ManifestFileFormatter_Verificaiton
      TestCaseSummary	  :	Verify Format of Manifest file if existed (Successfully generated manifest file)
      Steps			        :	1. Read Manifest file in the pilot specific location where it is generated
                          2. Check all the keys available as part of manifest file
      ExpectedResult	  :	Batch com.batch.creation service should generate successful batch manifest file with all keys
      Note: Due to following conditions batch manifest file may be written partially
      1. Due to failure in batch com.batch.creation service
      2. Due to failure in deletion of orphan stated manifest file or due to any runtime exception
   */

    UUID batchId = null;

    public void validate(JsonObject batchConfig) {

        List<String> manifestObjs = (List<String>) VariableCollections.map.get("manifestObjs");


        for (String manifestObject : manifestObjs) {
            JsonObject mObj = ManifestFileParser.getManifestFile(manifestObject);
            ManifestResponse mfResponse = ManifestFileParser.getManifestResponse(mObj);


            Boolean is_BatchObjects_Available = mObj.has("batchObjects");
            Boolean is_BatchSizeInMb_Available = mObj.has("batchSizeInBytes");
            Boolean is_Batch_Id_Available = mObj.has("batchId");
            Boolean is_BatchCreationTime_Available = mObj.has("batchCreationTime");
            Boolean is_BatchCreationType_Available = mObj.has("batchCreationType");
            Boolean is_latestObjectModifiedTime_Available = mObj.has("latestObjectModifiedTime");
            Boolean is_latestObjectKey_Available = mObj.has("latestObjectKey");
            Boolean is_emr_params_Available = mObj.has("emr_params");
            Boolean is_stepConfigs_Available = mObj.has("stepConfigs");
            Boolean is_scheduling_Available = mObj.has("scheduling");
            if (is_BatchObjects_Available && is_BatchSizeInMb_Available && is_Batch_Id_Available && is_Batch_Id_Available && is_BatchCreationTime_Available && is_BatchCreationType_Available && is_latestObjectModifiedTime_Available && is_latestObjectKey_Available && is_emr_params_Available && is_stepConfigs_Available && is_scheduling_Available) {
                Reporter.log("All keys are available in the Manifest file " + manifestObject, true);
            } else if (!is_BatchObjects_Available) {
                Reporter.log("Batch Objects are missing in the file" + manifestObject, true);
            } else if (!is_BatchSizeInMb_Available) {
                Reporter.log("Batch Size is missing in he file " + manifestObject, true);
            } else if (!is_Batch_Id_Available) {
                Reporter.log("Batch Id is missing in the file " + manifestObject, true);
            } else if (!is_BatchCreationTime_Available) {
                Reporter.log("Batch Creation time is invalid or missing in the file " + manifestObject, true);
            } else if (!is_BatchCreationType_Available) {
                Reporter.log("Batch Creation Type is invalid or missing in the file " + manifestObject, true);
            } else if (!is_latestObjectModifiedTime_Available) {
                Reporter.log("latest object modified time is invalid or missing in the file " + manifestObject, true);
            } else if (!is_latestObjectKey_Available) {
                Reporter.log("latest object key is invalid or missing in the file " + manifestObject, true);
            } else if (!is_emr_params_Available) {
                Reporter.log("Batch EMR params are invalid or missing in the file " + manifestObject, true);
            } else if (!is_stepConfigs_Available) {
                Reporter.log("Batch Steps are invalid or missing in the file " + manifestObject, true);
            } else if (!is_scheduling_Available) {
                Reporter.log("Batch com.batch.scheduling is invalid or missing in the file " + manifestObject, true);
            } else if (!is_BatchObjects_Available) {
                Reporter.log("Batch Objects are missing in the file " + manifestObject, true);
            }

            if (is_Batch_Id_Available) {
                batchId = UUID.fromString(mObj.get("batchId").getAsString());
            }


        }


    }


  /*def validate(s3ManifestObjsPath: String,mObj: String): UUID = {
    val logger: Logger = LoggerFactory.getLogger(getClass.getSimpleName)
    Reporter.log("Validating file format of the Manifest file {}",mObj)
    val json_data = BatchManifestFileHandler.getManifestData(s3ManifestObjsPath,mObj)
    val jsonobj = new JSONObject(json_data)
    val is_BatchObjects_Available = jsonobj.has("batchObjects")
    val is_BatchSizeInMb_Available = jsonobj.has("batchSizeInBytes")
    val is_Batch_Id_Available = jsonobj.has("batchId")
    val is_BatchCreationTime_Available = jsonobj.has("batchCreationTime")
    val is_BatchCreationType_Available = jsonobj.has("batchCreationType")
    val is_latestObjectModifiedTime_Available = jsonobj.has("latestObjectModifiedTime")
    val is_latestObjectKey_Available = jsonobj.has("latestObjectKey")
    val is_emr_params_Available = jsonobj.has("emr_params")
    val is_stepConfigs_Available = jsonobj.has("stepConfigs")
    val is_scheduling_Available = jsonobj.has("com.batch.scheduling")


    if (is_BatchObjects_Available && is_BatchSizeInMb_Available && is_Batch_Id_Available
      && is_Batch_Id_Available && is_BatchCreationTime_Available && is_BatchCreationType_Available
      && is_latestObjectModifiedTime_Available && is_latestObjectKey_Available
      && is_emr_params_Available && is_stepConfigs_Available && is_scheduling_Available) {
      Reporter.log("All keys are available in the Manifest file {} - Test case Passed !",mObj)
    } else if (!is_BatchObjects_Available) {
     Reporter.log("Batch Objects are missing in the file {} - Test case Failed !",mObj)
    } else if (!is_BatchSizeInMb_Available) {
     Reporter.log("Batch Size is missing in he file {} - Test case Failed !",mObj)
    } else if (!is_Batch_Id_Available) {
     Reporter.log("Batch Id is missing in the file {} - Test case Failed !",mObj)
    } else if (!is_BatchCreationTime_Available) {
     Reporter.log("Batch Creation time is invalid or missing in the file {} - Test case Failed !",mObj)
    } else if (!is_BatchCreationType_Available) {
     Reporter.log("Batch Creation Type is invalid or missing in the file {} - Test case Failed !",mObj)
    } else if (!is_latestObjectModifiedTime_Available) {
     Reporter.log("latest object modified time is invalid or missing in the file {} - Test case Failed !",mObj)
    } else if (!is_latestObjectKey_Available) {
     Reporter.log("latest object key is invalid or missing in the file {} - Test case Failed !",mObj)
    } else if (!is_emr_params_Available) {
     Reporter.log("Batch EMR params are invalid or missing in the file {} - Test case Failed !",mObj)
    } else if (!is_stepConfigs_Available) {
     Reporter.log("Batch Steps are invalid or missing in the file {} - Test case Failed !",mObj)
    } else if (!is_scheduling_Available) {
     Reporter.log("Batch com.batch.scheduling is invalid or missing in the file {} - Test case Failed !",mObj)
    } else if (!is_BatchObjects_Available) {
     Reporter.log("Batch Objects are missing in the file {} - Test case Failed !",mObj)
    }

    if (is_Batch_Id_Available) {
      batchId = UUID.fromString(jsonobj.get("batchId").toString)
    }
    batchId //returning batchId
  }
*/

}
