package com.batch.creation;

import com.batch.utils.ManifestFileParser;
import com.batch.utils.ManifestResponse;
import com.google.gson.JsonObject;
import org.testng.Assert;
import org.testng.Reporter;

import java.util.UUID;

/**
 * @author Rama Kalyan
 */
public class ValidateManifestFile {

    UUID batchId = null;


    public void ManifestFileValidation(String manifestObject) {

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
        }  if (!is_BatchObjects_Available) {
            Reporter.log("Batch Objects are missing in the file" + manifestObject, true);
        }  if (!is_BatchSizeInMb_Available) {
            Reporter.log("Batch Size is missing in he file " + manifestObject, true);
        }  if (!is_Batch_Id_Available) {
            Reporter.log("Batch Id is missing in the file " + manifestObject, true);
        }  if (!is_BatchCreationTime_Available) {
            Reporter.log("Batch Creation time is invalid or missing in the file " + manifestObject, true);
        }  if (!is_BatchCreationType_Available) {
            Reporter.log("Batch Creation Type is invalid or missing in the file " + manifestObject, true);
        }  if (!is_latestObjectModifiedTime_Available) {
            Reporter.log("latest object modified time is invalid or missing in the file " + manifestObject, true);
        }  if (!is_latestObjectKey_Available) {
            Reporter.log("latest object key is invalid or missing in the file " + manifestObject, true);
        }  if (!is_emr_params_Available) {
            Reporter.log("Batch EMR params are invalid or missing in the file " + manifestObject, true);
        }  if (!is_stepConfigs_Available) {
            Reporter.log("Batch Steps are invalid or missing in the file " + manifestObject, true);
        }  if (!is_scheduling_Available) {
            Reporter.log("Batch com.batch.scheduling is invalid or missing in the file " + manifestObject, true);
        }  if (!is_BatchObjects_Available) {
            Reporter.log("Batch Objects are missing in the file " + manifestObject, true);
        }



        //extract below
        if (is_Batch_Id_Available) {
            batchId = UUID.fromString(mObj.get("batchId").getAsString());
            Boolean DBEntryAvailability = DBEntryVerification.validate(batchId);
            Assert.assertEquals(DBEntryAvailability, true);
        }


        // Compare common kys between config and manifest file
        //extract batchId from manifestkey path - > compare with batchId value

        // validate scheduling config and all components



    }
}
