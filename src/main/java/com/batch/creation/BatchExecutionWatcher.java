package com.batch.creation;


import java.util.Calendar;

public class BatchExecutionWatcher {


    public static void bewatch(int baseMinute) {
        boolean waitFlag = true;
        while (waitFlag) {
            Calendar date = Calendar.getInstance();
            int CalMinute = date.get(Calendar.MINUTE);
            System.out.println(date.getTime() + " -> waiting for Batch Creation Service to complete !");
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
       /* public static void printProgressBar(double pct) {
            // if bar_size changes, then change erase_bar (in eraseProgressBar) to
            // match.
            final int bar_size = 40;
            final String empty_bar = "                                        ";
            final String filled_bar = "########################################";
            int amt_full = (int) (bar_size * (pct / 100.0));
            System.out.format("  [%s%s] %.2f ", filled_bar.substring(0, amt_full), empty_bar.substring(0, bar_size - amt_full), pct);
        }*/
    }
}