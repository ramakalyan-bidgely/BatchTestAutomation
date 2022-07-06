package com.batch.utils;

public class InputConfig {

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
    private int maxLookUpDays;
    private int dataSizeInBytes;
    private int intervalInSec;
    private boolean skipSucceededTasksOnRetry;
    private boolean isNextBatchDependentOnPrev;
    private int parallelBatchesIfIndependent;
    private int maxTries;
    private String dagId;

    public String getComponent() {
        return component;
    }

    public void setComponent(String component) {
        this.component = component;
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

    public int getMaxLookUpDays() {
        return maxLookUpDays;
    }

    public void setMaxLookUpDays(int maxLookUpDays) {
        this.maxLookUpDays = maxLookUpDays;
    }

    public Long getDataSizeInBytes() {
        return Long.valueOf(dataSizeInBytes);
    }

    public void setDataSizeInBytes(int dataSizeInBytes) {
        this.dataSizeInBytes = dataSizeInBytes;
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




}
