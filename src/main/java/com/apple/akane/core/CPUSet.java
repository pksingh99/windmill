package com.apple.akane.core;

import java.nio.channels.SocketChannel;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import com.apple.akane.core.tasks.VoidTask;

import com.apple.akane.net.Channel;
import com.google.common.collect.*;

public class CPUSet
{
    private final ImmutableMap<Integer, Socket> sockets;
    private final ImmutableList<CPU> cpus;

    private CPUSet(ImmutableMap<Integer, Socket> sockets)
    {
        ImmutableList.Builder<CPU> cpus = ImmutableList.builder();
        for (Socket socket : sockets.values())
            cpus.addAll(socket.cpus);

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

        public Builder addSocket(int... cpuIds)
        {
            sockets.put(socketId++, new Socket(cpuIds));
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

        private Socket(int... cpuIds)
        {
            ImmutableList.Builder<CPU> cpus = ImmutableList.builder();
            for (int cpuId : cpuIds)
                cpus.add(new CPU(cpuId, this));

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

        public void register(SocketChannel channel, VoidTask<Channel> onSuccess, VoidTask<Throwable> onFailure)
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
