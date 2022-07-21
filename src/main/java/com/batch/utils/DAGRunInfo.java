package com.batch.utils;

public class DAGRunInfo {
    private String dag_run_id;
    private String dag_id;
    private String logicalDate;
    private String executionDate;
    private String startDate;
    private String endDate;
    private String state;
    private boolean externalTrigger;
    private Object conf;
    public String getDag_run_id() {
        return dag_run_id;
    }

    public void setDag_run_id(String dag_run_id) {
        this.dag_run_id = dag_run_id;
    }

    public String getDag_id() {
        return dag_id;
    }

    public void setDag_id(String dag_id) {
        this.dag_id = dag_id;
    }

    public String getLogicalDate() {
        return logicalDate;
    }

    public void setLogicalDate(String logicalDate) {
        this.logicalDate = logicalDate;
    }

    public String getExecutionDate() {
        return executionDate;
    }

    public void setExecutionDate(String executionDate) {
        this.executionDate = executionDate;
    }

    public String getStartDate() {
        return startDate;
    }

    public void setStartDate(String startDate) {
        this.startDate = startDate;
    }

    public String getEndDate() {
        return endDate;
    }

    public void setEndDate(String endDate) {
        this.endDate = endDate;
    }

    public String getState() {
        return state;
    }

    public void setState(String state) {
        this.state = state;
    }

    public boolean isExternalTrigger() {
        return externalTrigger;
    }

    public void setExternalTrigger(boolean externalTrigger) {
        this.externalTrigger = externalTrigger;
    }

    public Object getConf() {
        return conf;
    }

    public void setConf(Object conf) {
        this.conf = conf;
    }


}
