package com.hansight.kunlun.collector.utils;

import org.junit.Test;

import java.util.HashMap;
import java.util.concurrent.*;

/**
 * Author:zhhui
 * DateTime:2014/8/13 17:28.
 */
public class ExecutorTest {
    @Test
    public void InterruptedTest() {

        ExecutorService executor = Executors.newSingleThreadExecutor();
        final FutureTask<String> futureTask = new FutureTask<String>(
                new Callable<String>() {
                    public String call() {
                        try {
                            Thread.sleep(500);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                        return "";
                    }
                });
        FutureTask<String> futureTask2 = new FutureTask<String>(
                new Callable<String>() {
                    public String call() {
                        try {
                            futureTask.get();
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        } catch (ExecutionException e) {
                            e.printStackTrace();
                        }
                        return "";
                    }
                });

        executor.execute(futureTask);
        executor.execute(futureTask2);

        String result = null;
        try {
            //   result = futureTask2.get();

           /* while (!futureTask2.isDone()) {
                futureTask.cancel(true);

            }*/
            // futureTask2.cancel(true);
            while (!executor.isTerminated()) {
                Thread.sleep(100);
                executor.shutdownNow();
                System.out.println("executor.isTerminated() = ");
            }

        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            System.out.println("closing...");
            executor.shutdown();
            System.out.println("closed.");
        }
        System.out.println("result=" + result);
    }

    static class MyThread implements Runnable {
        private String name;

        MyThread(String name) {
            this.name = "thread-" + name;
        }

        @Override
        public void run() {
            System.out.println(name + " :start ...");
            HashMap map=new HashMap();
            try {
                byte [] bytes=new byte[200_000];
                for(int i=0;i<100_000;i++)
                map.put(""+i,bytes);
                TimeUnit.SECONDS.sleep(10);
                map.put("","");
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            System.out.println(name + ":finished");
        }
    }

    @Test
    public void threadPoolTest() {
        // 创建一个可重用固定线程数的线程池


        ExecutorService pool = Executors.newFixedThreadPool(1);

        // 创建实现了Runnable接口对象，Thread对象当然也实现了Runnable接口
        for (int i = 0; i < 1000_000; i++) {
            pool.execute(new MyThread("" + i));
        }
        System.out.println(pool + " :finished");
        // 关闭线程池
        while (!pool.isShutdown()) {
            try {
                TimeUnit.SECONDS.sleep(1);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        pool.shutdown();


    }
}
