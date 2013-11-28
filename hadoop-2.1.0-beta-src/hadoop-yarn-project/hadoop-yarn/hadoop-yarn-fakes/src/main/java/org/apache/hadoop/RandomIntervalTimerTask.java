package org.apache.hadoop;

import java.util.Random;
import java.util.Timer;
import java.util.TimerTask;

public class RandomIntervalTimerTask extends TimerTask {

    final Runnable task;
    final Random rnd;
    final int avgPeriodSecond;
    final Timer timer = new Timer();

    public RandomIntervalTimerTask(Runnable task, Random rnd,
            int avgPeriodSecond) {
        this.task = task;
        this.rnd = rnd;
        this.avgPeriodSecond = avgPeriodSecond;
    }

    @Override
    public void run() {
        // TODO Auto-generated method stub
        task.run();
        timer.schedule(
                new RandomIntervalTimerTask(task, rnd, avgPeriodSecond),
                1000 * Math.max(
                        5,
                        (int) Math.ceil((avgPeriodSecond / 5.0)
                                * rnd.nextGaussian() + avgPeriodSecond)));
    }
}
