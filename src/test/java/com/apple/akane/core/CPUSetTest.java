package com.apple.akane.core;

import java.util.concurrent.ThreadLocalRandom;

import org.junit.Assert;
import org.junit.Test;

public class CPUSetTest
{
    @Test
    public void testGetCPUs()
    {
        CPUSet cpus = CPUSet.builder().addPack(0, 1, 2).addPack(3, 4, 5).build();
        for (int i = 0; i < 6; i++)
            Assert.assertNotNull(cpus.get(ThreadLocalRandom.current().nextInt(0, 6)));
    }

    @Test
    public void testNUMANode()
    {
        CPUSet cpus = CPUSet.builder().addPack(0, 1).addPack(2, 3, 4).build();

        CPUSet.Pack pack0 = cpus.getNUMANode(0);
        CPUSet.Pack pack1 = cpus.getNUMANode(1);

        Assert.assertEquals(2, pack0.size());
        Assert.assertEquals(3, pack1.size());

        Assert.assertEquals(0, pack0.getCPU(0).id);
        Assert.assertEquals(1, pack0.getCPU(1).id);


        Assert.assertEquals(2, pack1.getCPU(0).id);
        Assert.assertEquals(3, pack1.getCPU(1).id);
        Assert.assertEquals(4, pack1.getCPU(2).id);
    }
}
