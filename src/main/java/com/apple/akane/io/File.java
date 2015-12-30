package com.apple.akane.io;

import java.io.RandomAccessFile;

import com.apple.akane.core.Future;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

public class File
{
    private final IOService io;
    private final RandomAccessFile backend;

    File(IOService ioService, RandomAccessFile file)
    {
        this.io = ioService;
        this.backend = file;
    }

    public Future<Void> write(byte[] buffer)
    {
        return io.schedule(() -> {
            backend.write(buffer);
            return null;
        });
    }

    public Future<ByteBuf> read(int size)
    {
        return io.schedule(() -> {
            byte[] buffer = new byte[size];
            backend.readFully(buffer);

            return Unpooled.wrappedBuffer(buffer);
        });
    }

    public Future<Void> seek(long position)
    {
        return io.schedule(() -> {
            backend.seek(position);
            return null;
        });
    }

    public Future<Void> sync()
    {
        return io.schedule(() -> {
            backend.getFD().sync();
            return null;
        });
    }

    public Future<Void> close()
    {
        return io.schedule(() -> {
            backend.close();
            return null;
        });
    }
}
