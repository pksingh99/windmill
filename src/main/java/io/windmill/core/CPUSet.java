package io.windmill.core;

import java.io.IOException;
import java.nio.channels.ServerSocketChannel;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;

import io.windmill.disk.PageRef;
import io.windmill.disk.cache.Page;
import io.windmill.net.Channel;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.RemovalCause;

import net.openhft.affinity.CpuLayout;
import net.openhft.affinity.impl.VanillaCpuLayout;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CPUSet
{
    private static final Logger logger = LoggerFactory.getLogger(CPUSet.class);

    public static final int DEFAULT_PAGE_CACHE_SIZE =  512 * 1024 * 1024;  // 512 MB by default

    private final Map<Integer, Socket> sockets;
    private final Map<Integer, CPU> cpus;

    private CPUSet(Map<Integer, Socket> sockets)
    {
        Map<Integer, CPU> cpus = new HashMap<>();
        for (Socket socket : sockets.values())
        {
            for (CPU cpu : socket.cpus)
                cpus.put(cpu.id, cpu);
        }

        this.sockets = sockets;
        this.cpus = Collections.unmodifiableMap(cpus);
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

        private final Map<Integer, int[]> sockets = new HashMap<>();
        private final CpuLayout layout;

        protected long pageCacheSize = DEFAULT_PAGE_CACHE_SIZE;

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

            sockets.put(socketId++, cpuIds);
            return this;
        }

        public Builder setPageCacheSize(long pageCacheSize)
        {
            if (pageCacheSize <= 0)
                throw new IllegalArgumentException("page cache size should be positive");

            this.pageCacheSize = pageCacheSize;
            return this;
        }

        public CPUSet build()
        {
            Cache<PageRef, Boolean> pageTracker = Caffeine.<PageRef, Boolean>newBuilder()
                                                          .maximumSize(pageCacheSize / Page.PAGE_SIZE)
                                                          .removalListener((PageRef page, Boolean v, RemovalCause cause) -> page.evict())
                                                          .build();

            Map<Integer, Socket> cpuSet = new HashMap<>();
            for (Map.Entry<Integer, int[]> socket : sockets.entrySet())
                cpuSet.put(socket.getKey(), new Socket(layout, pageTracker, socket.getValue()));

            return new CPUSet(Collections.unmodifiableMap(cpuSet));
        }
    }

    public static class Socket
    {
        private final List<CPU> cpus;

        private Socket(CpuLayout layout, Cache<PageRef, Boolean> pageTracker, int... cpuIds)
        {
            List<CPU> cpus = new ArrayList<>(cpuIds.length);
            for (int cpuId : cpuIds)
                cpus.add(new CPU(layout, cpuId, this, pageTracker));

            this.cpus = Collections.unmodifiableList(cpus);
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

        public void register(ServerSocketChannel channel, Future<Channel> promise)
        {
            if (channel == null)
                return;

            CPU cpu = getCPU();

            try
            {
                promise.setValue(new Channel(cpu, cpu.getSelector(), channel.accept()));
            }
            catch (Exception | Error e)
            {
                promise.setFailure(e);
            }
        }
    }
}
