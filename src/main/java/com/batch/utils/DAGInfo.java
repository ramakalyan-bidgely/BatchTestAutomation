package com.batch.utils;


import java.sql.Timestamp;
import java.util.List;

public class DAGInfo {
    private String dag_id;
    private String description;
    private String file_token;
    private String fileloc;
    private boolean is_active;
    private boolean is_paused;
    private boolean is_subdag;
    private List<String> tags;
    private List<String> owners;
    private String root_dag_id;
    private Object schedule_interval;
    public String getDag_id() {
        return dag_id;
    }

    public void setDag_id(String dag_id) {
        this.dag_id = dag_id;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public String getFile_token() {
        return file_token;
    }

    public void setFile_token(String file_token) {
        this.file_token = file_token;
    }

    public String getFileloc() {
        return fileloc;
    }

    public void setFileloc(String fileloc) {
        this.fileloc = fileloc;
    }

    public boolean getIs_active() {
        return is_active;
    }

    public void setIs_active(boolean is_active) {
        this.is_active = is_active;
    }

    public boolean getIs_paused() {
        return is_paused;
    }

    public void setIs_paused(boolean is_paused) {
        this.is_paused = is_paused;
    }

    public boolean getIs_subdag() {
        return is_subdag;
    }

    public void setIs_subdag(boolean is_subdag) {
        this.is_subdag = is_subdag;
    }

    public List<String> getTags() {
        return tags;
    }

    public void setTags(List<String> tags) {
        this.tags = tags;
    }

    public List<String> getOwners() {
        return owners;
    }

    public void setOwners(List<String> owners) {
        this.owners = owners;
    }



    public String getRoot_dag_id() {
        return root_dag_id;
    }

    public void setRoot_dag_id(String root_dag_id) {
        this.root_dag_id = root_dag_id;
    }



    public Object getSchedule_interval() {
        return schedule_interval;
    }

    public void setSchedule_interval(Object schedule_interval) {
        this.schedule_interval = schedule_interval;
    }



}

