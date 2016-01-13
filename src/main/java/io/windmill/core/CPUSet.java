package io.windmill.core;

import java.io.IOException;
import java.nio.channels.SocketChannel;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import io.windmill.core.tasks.VoidTask1;
import io.windmill.net.Channel;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.*;

import net.openhft.affinity.CpuLayout;
import net.openhft.affinity.impl.VanillaCpuLayout;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CPUSet
{
    private static final Logger logger = LoggerFactory.getLogger(CPUSet.class);

    private final ImmutableMap<Integer, Socket> sockets;
    private final ImmutableMap<Integer, CPU> cpus;

    private CPUSet(ImmutableMap<Integer, Socket> sockets)
    {
        ImmutableMap.Builder<Integer, CPU> cpus = ImmutableMap.builder();
        for (Socket socket : sockets.values())
        {
            for (CPU cpu : socket.cpus)
                cpus.put(cpu.id, cpu);
        }

        this.sockets = sockets;
        this.cpus = cpus.build();
    }

    public CPU get(int cpuId)
    {
        return cpus.get(cpuId);
    }

    public Socket getSocket(int id)
    {
        return sockets.get(id);
    }

    public void start()
    {
        sockets.values().stream().forEach(Socket::start);
    }

    public void halt()
    {
        sockets.values().stream().forEach(Socket::halt);
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public static class Builder
    {
        private int socketId = 0;
        private final ImmutableMap.Builder<Integer, Socket> sockets = ImmutableMap.builder();
        private final CpuLayout layout;

        public Builder()
        {
            CpuLayout layout;

            try
            {
                layout = VanillaCpuLayout.fromCpuInfo();
            }
            catch (IOException e)
            {
                logger.warn("Failed to determine CPU layout, affinity adjustments are going to be disabled.");
                layout = null;
            }

            this.layout = layout;
        }

        @VisibleForTesting
        protected Builder(CpuLayout layout)
        {
            this.layout = layout;
        }

        public Builder addSocket(int... cpuIds)
        {
            if (layout != null)
            {
                if (layout.sockets() < socketId + 1)
                    throw new IllegalArgumentException(String.format("Insufficient CPU sockets, total %d, tried to add one more.", layout.sockets()));

                if (layout.coresPerSocket() < cpuIds.length)
                    throw new IllegalArgumentException(String.format("Insufficient CPU cores per socket, total %d, tried to allocate %d.", layout.coresPerSocket(), cpuIds.length));
            }

            sockets.put(socketId++, new Socket(layout, cpuIds));
            return this;
        }

        public CPUSet build()
        {
            return new CPUSet(sockets.build());
        }
    }

    public static class Socket
    {
        private final List<CPU> cpus;

        private Socket(CpuLayout layout, int... cpuIds)
        {
            ImmutableList.Builder<CPU> cpus = ImmutableList.builder();
            for (int cpuId : cpuIds)
                cpus.add(new CPU(layout, cpuId, this));

            this.cpus = cpus.build();
        }

        public void start()
        {
            cpus.stream().forEach(CPU::start);
        }

        public void halt()
        {
            cpus.stream().forEach(CPU::halt);
        }

        public CPU getCPU()
        {
            int size = size();
            return size == 1 ? cpus.get(0) : cpus.get(ThreadLocalRandom.current().nextInt(0, size - 1));
        }

        public CPU getCPU(int id)
        {
            return cpus.get(id);
        }

        public int size()
        {
            return cpus.size();
        }

        public void register(SocketChannel channel, VoidTask1<Channel> onSuccess, VoidTask1<Throwable> onFailure)
        {
            if (channel == null)
                return;

            CPU cpu = getCPU();
            cpu.schedule(() -> {
                try
                {
                    onSuccess.compute(new Channel(cpu, cpu.getSelector(), channel));
                }
                catch (Exception | Error e)
                {
                    onFailure.compute(e);
                }

                return null;
            });
        }
    }
}
