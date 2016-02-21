package io.windmill.utils;

import java.io.FileDescriptor;
import java.lang.reflect.Field;

import com.sun.jna.LastErrorException;
import com.sun.jna.Native;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Most of the methods here are taken from org.apache.cassandra.utils.CLibrary >= v3.0
 */
public class CLibrary
{
    private static final Logger logger = LoggerFactory.getLogger(CLibrary.class);

    private static final int F_SETFL  = 4;      /* set file status flags */
    private static final int O_DIRECT = 040000; /* fcntl.h */

    private enum Platform
    {
        LINUX(true), OTHER(false);

        private final boolean supportsDirectIO;

        Platform(boolean directIO)
        {
            supportsDirectIO = directIO;
        }
    }

    private static final Platform PLATFORM;
    private static final boolean JNA_AVAILABLE;

    static
    {
        if (System.getProperty("os.name").toLowerCase().contains("linux"))
        {
            PLATFORM = Platform.LINUX;
        }
        else
        {
            PLATFORM = Platform.OTHER;
        }

        boolean jnaAvailable = false;

        try
        {
            Native.register("c");
            jnaAvailable = true;
        }
        catch (NoClassDefFoundError e)
        {
            logger.warn("JNA not found. Native methods will be disabled.");
        }
        catch (UnsatisfiedLinkError e)
        {
            logger.warn("JNA link failure, one or more native method will be unavailable.");
            logger.debug("JNA link failure details: {}", e.getMessage());
        }
        catch (NoSuchMethodError e)
        {
            logger.warn("Obsolete version of JNA present; unable to register C library. Upgrade to JNA 3.2.7 or later");
        }

        JNA_AVAILABLE = jnaAvailable;
    }

    private static native int fcntl(int fd, int command, long flags) throws LastErrorException;

    /**
     * Set O_DIRECT flag on the given file descriptor using fcntl syscall, enabling DMA access to the disk device.
     *
     * @param fileDescriptor The file descriptor to set O_DIRECT flag on.
     *
     * @return true if flag was set successfully and file is not in DMA mode, false otherwise.
     */
    public static boolean enableDirectIO(FileDescriptor fileDescriptor)
    {
        if (!JNA_AVAILABLE || !PLATFORM.supportsDirectIO)
            return false;

        int fd = getfd(fileDescriptor);
        return fd != -1 && tryFcntl(fd, F_SETFL, O_DIRECT) == 0;
    }

    private static int errno(RuntimeException e)
    {
        assert e instanceof LastErrorException;

        try
        {
            return ((LastErrorException) e).getErrorCode();
        }
        catch (NoSuchMethodError x)
        {
            logger.warn("Obsolete version of JNA present; unable to read errno. Upgrade to JNA 3.2.7 or later");
            return 0;
        }
    }

    private static int tryFcntl(int fd, int command, int flags)
    {
        // fcntl return value may or may not be useful, depending on the command
        int result = -1;

        if (!JNA_AVAILABLE)
            return result;

        try
        {
            result = fcntl(fd, command, flags);
        }
        catch (UnsatisfiedLinkError e)
        {
            // if JNA is unavailable just skipping
        }
        catch (RuntimeException e)
        {
            if (!(e instanceof LastErrorException))
                throw e;

            logger.warn(String.format("fcntl(%d, %d, %d) failed, errno (%d).", fd, command, flags, errno(e)));
        }

        return result;
    }

    /**
     * Get system file descriptor from FileDescriptor object.
     * @param descriptor - FileDescriptor objec to get fd from
     * @return file descriptor, -1 or error
     */
    public static int getfd(FileDescriptor descriptor)
    {
        Field field = getProtectedField(descriptor.getClass(), "fd");

        if (field == null)
            return -1;

        try
        {
            return field.getInt(descriptor);
        }
        catch (Exception e)
        {
            logger.warn("unable to read fd field from FileDescriptor");
        }

        return -1;
    }

    /**
     * Used to get access to protected/private field of the specified class
     * @param klass - name of the class
     * @param fieldName - name of the field
     * @return Field or null on error
     */
    private static Field getProtectedField(Class klass, String fieldName)
    {
        Field field;

        try
        {
            field = klass.getDeclaredField(fieldName);
            field.setAccessible(true);
        }
        catch (Exception e)
        {
            throw new AssertionError(e);
        }

        return field;
    }


    private CLibrary()
    {}
}
