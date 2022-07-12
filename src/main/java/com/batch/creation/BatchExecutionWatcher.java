package com.batch.creation;


import org.testng.Reporter;

import java.util.Calendar;

import static java.lang.Math.floorDiv;

public class BatchExecutionWatcher {


    public static void bewatch(int triggerPointVal) {
        boolean waitFlag = true;
        Calendar dt = Calendar.getInstance();

        while (waitFlag) {
            Calendar date = Calendar.getInstance();
            int CalMinute = date.get(Calendar.MINUTE);
            int CalSecond = date.get(Calendar.SECOND);
            //int ElapseMinute = (floorDiv(CalMinute, 10) + 10 + triggerPoint) - CalMinute;
            Reporter.log(date.getTime() + " -> waiting for Batch Creation Service to complete !", true);
            if (CalMinute % 10 == triggerPointVal) {
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