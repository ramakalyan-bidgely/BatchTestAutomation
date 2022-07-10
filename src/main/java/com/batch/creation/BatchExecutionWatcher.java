package com.batch.creation;


import java.util.Calendar;

public class BatchExecutionWatcher {


    public static void bewatch(int baseMinute) {
        boolean waitFlag = true;
        while (waitFlag) {
            Calendar date = Calendar.getInstance();
            int CalMinute = date.get(Calendar.MINUTE);
            System.out.println(date.getTime() + " -> waiting for Batch Creation Service to.. ");
            if (CalMinute % 10 == baseMinute) {
                waitFlag = false;
                continue;
            } else {
                try {
                    Thread.sleep(10000);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }
}