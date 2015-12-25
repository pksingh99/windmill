package com.apple.akane.core;

import java.util.concurrent.ThreadLocalRandom;

import org.junit.Assert;
import org.junit.Test;

public class CPUSetTest
{
    @Test
    public void testGetCPUs()
    {
        CPUSet cpus = CPUSet.builder().addSocket(0, 1, 2).addSocket(3, 4, 5).build();
        for (int i = 0; i < 6; i++)
            Assert.assertNotNull(cpus.get(ThreadLocalRandom.current().nextInt(0, 6)));
    }

    @Test
    public void testNUMANode()
    {
        CPUSet cpus = CPUSet.builder().addSocket(0, 1).addSocket(2, 3, 4).build();

        CPUSet.Socket socket0 = cpus.getSocket(0);
        CPUSet.Socket socket1 = cpus.getSocket(1);

        Assert.assertEquals(2, socket0.size());
        Assert.assertEquals(3, socket1.size());

        Assert.assertEquals(0, socket0.getCPU(0).id);
        Assert.assertEquals(1, socket0.getCPU(1).id);


        Assert.assertEquals(2, socket1.getCPU(0).id);
        Assert.assertEquals(3, socket1.getCPU(1).id);
        Assert.assertEquals(4, socket1.getCPU(2).id);
    }
}
