package com.batch.utils;

import com.google.gson.JsonArray;

public class ManifestResponse {

    private String component;
    private int pilotId;
    private int workerInstanceCount;
    private int diskSizeInGb;
    private int volumePerInstance;
    private String bucket;
    private String prefix;
    private String directoryStructure;
    private String datasetType;
    private String dataFormat;
    private String compressionFormat;
    private Long batchSizeInBytes;

    private Long batchCreationTime;

    private String batchCreationType;

    private Long latestObjectModifiedTime;

    private String latestObjectKey;
    private int intervalInSec;
    private boolean skipSucceededTasksOnRetry;
    private boolean isNextBatchDependentOnPrev;
    private int parallelBatchesIfIndependent;
    private int maxTries;
    private String dagId;
    private String clusterName;

    private String batchId;
    private static JsonArray batchObjects;


    public static JsonArray getBatchObjects() {
        return batchObjects;
    }

    public void setBatchObjects(JsonArray batchObjects) {
        this.batchObjects = batchObjects;
    }

    public String getComponent() {
        return component;
    }


    public void setComponent(String component) {
        this.component = component;
    }

    public String getLatestObjectKey() {
        return latestObjectKey;
    }

    public Long getlatestObjectModifiedTime() {
        return latestObjectModifiedTime;
    }

    public Long getbatchCreationTime() {
        return batchCreationTime;
    }

    public String getbatchCreationType() {
        return batchCreationType;
    }

    public int getPilotId() {
        return pilotId;
    }

    public void setPilotId(int pilotId) {
        this.pilotId = pilotId;
    }

    public int getWorkerInstanceCount() {
        return workerInstanceCount;
    }

    public void setWorkerInstanceCount(int workerInstanceCount) {
        this.workerInstanceCount = workerInstanceCount;
    }

    public int getDiskSizeInGb() {
        return diskSizeInGb;
    }

    public void setDiskSizeInGb(int diskSizeInGb) {
        this.diskSizeInGb = diskSizeInGb;
    }

    public int getVolumePerInstance() {
        return volumePerInstance;
    }

    public void setVolumePerInstance(int volumePerInstance) {
        this.volumePerInstance = volumePerInstance;
    }

    public String getBucket() {
        return bucket;
    }

    public void setBucket(String bucket) {
        this.bucket = bucket;
    }

    public String getPrefix() {
        return prefix;
    }

    public String getClusterName() {
        return clusterName;
    }

    public void setClusterName(String clusterName) {
        this.clusterName = clusterName;
    }

    ;

    public void setPrefix(String prefix) {
        this.prefix = prefix;
    }

    public String getDirectoryStructure() {
        return directoryStructure;
    }

    public void setDirectoryStructure(String directoryStructure) {
        this.directoryStructure = directoryStructure;
    }

    public String getDatasetType() {
        return datasetType;
    }

    public void setDatasetType(String datasetType) {
        this.datasetType = datasetType;
    }

    public String getDataFormat() {
        return dataFormat;
    }

    public void setDataFormat(String dataFormat) {
        this.dataFormat = dataFormat;
    }

    public String getCompressionFormat() {
        return compressionFormat;
    }

    public void setCompressionFormat(String compressionFormat) {
        this.compressionFormat = compressionFormat;
    }

    public Long getbatchSizeInBytes() {
        return batchSizeInBytes;
    }

    public void setDataSizeInBytes(Long dataSizeInBytes) {
        this.batchSizeInBytes = dataSizeInBytes;
    }

    public int getIntervalInSec() {
        return intervalInSec;
    }

    public void setIntervalInSec(int intervalInSec) {
        this.intervalInSec = intervalInSec;
    }

    public boolean isSkipSucceededTasksOnRetry() {
        return skipSucceededTasksOnRetry;
    }

    public void setSkipSucceededTasksOnRetry(boolean skipSucceededTasksOnRetry) {
        this.skipSucceededTasksOnRetry = skipSucceededTasksOnRetry;
    }

    public boolean isNextBatchDependentOnPrev() {
        return isNextBatchDependentOnPrev;
    }

    public void setNextBatchDependentOnPrev(boolean nextBatchDependentOnPrev) {
        isNextBatchDependentOnPrev = nextBatchDependentOnPrev;
    }

    public int getParallelBatchesIfIndependent() {
        return parallelBatchesIfIndependent;
    }

    public void setParallelBatchesIfIndependent(int parallelBatchesIfIndependent) {
        this.parallelBatchesIfIndependent = parallelBatchesIfIndependent;
    }

    public int getMaxTries() {
        return maxTries;
    }

    public void setMaxTries(int maxTries) {
        this.maxTries = maxTries;
    }

    public String getDagId() {
        return dagId;
    }

    public void setDagId(String dagId) {
        this.dagId = dagId;
    }

    public String getbatchId() {
        return batchId;
    }

    ;

    public void setbatchId(String batchId) {
        this.batchId = batchId;
    }

    public void setbatchCreationType(String batchCreationType) {
        this.batchCreationType = batchCreationType;
    }


    public void setbatchCreationTime(String batchCreationTime) {
        this.batchCreationTime = Long.valueOf(batchCreationTime);
    }

    public void setlatestObjectKey(String latestObjectKey) {
        this.latestObjectKey = latestObjectKey;
    }

    public void setlatestObjectModifiedTime(String latestObjectModifiedTime) {
        this.latestObjectModifiedTime = Long.valueOf(latestObjectModifiedTime);
    }

    public void setbatchSizeInBytes(long batchSizeInBytes) {
        this.batchSizeInBytes = batchSizeInBytes;
    }
}
