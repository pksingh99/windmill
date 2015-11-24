package com.apple.akane;

import com.apple.akane.core.CPU;
import com.apple.akane.core.tasks.Task0;

public class App 
{
    public static void main(String[] args) throws Exception
    {
        CPU cpu0 = new CPU();
        CPU cpu1 = new CPU();

        new Thread(cpu0).start();
        new Thread(cpu1).start();

        cpu0.schedule(sum(new int[] { 1, 2, 3 })).onSuccess(System.out::println);
        cpu1.schedule(sum(new int[] { 4, 5, 6 })).onSuccess(System.out::println);
        cpu0.schedule(sum(new int[] { 7, 8, 9 })).onSuccess(System.out::println);

        cpu1.schedule(sum(new int[] { 10, 11, 12 })).map((sum) -> Integer.toString(sum)).onSuccess(System.out::println);
        cpu0.schedule(sum(new int[] { 10, 11, 12 })).map(cpu1, (sum) -> Integer.toString(sum)).onSuccess(System.out::println);
        cpu0.schedule(sum(new int[] { 13, 14, 15 })).flatMap((sum) -> cpu1.schedule(() -> 42)).onSuccess(System.out::println);

        Thread.currentThread().join();

        System.exit(0);
    }

    public static Task0<Integer> sum(int[] numbers)
    {
        return () -> {
            int sum = 0;
            for (int n : numbers)
                sum += n;

            return sum;
        };
    }
}