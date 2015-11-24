package com.apple.akane.core;

import org.junit.AfterClass;
import org.junit.BeforeClass;

public class AbstractTest
{
    protected static CPU CPU;

    @BeforeClass
    public static void before() throws Exception
    {
        CPU = new CPU();
        new Thread(CPU).start();
    }

    @AfterClass
    public static void after()
    {
        CPU.halt();
    }

    public static class ConstantFuture<T> extends Future<T>
    {
        public ConstantFuture(CPU cpu, T value)
        {
            super(cpu);
            setValue(value);
        }
    }
}
