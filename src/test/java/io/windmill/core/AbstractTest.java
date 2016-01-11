package io.windmill.core;

import org.junit.AfterClass;
import org.junit.BeforeClass;

public class AbstractTest
{
    protected static CPUSet CPUs;

    @BeforeClass
    public static void before() throws Exception
    {
        CPUs = CPUSet.builder().addSocket(0).addSocket(2).build();
        CPUs.start();
    }

    @AfterClass
    public static void after()
    {
        CPUs.halt();
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
