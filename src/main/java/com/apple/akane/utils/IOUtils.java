package com.apple.akane.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IOUtils
{
    private static final Logger logger = LoggerFactory.getLogger(IOUtils.class);

    private IOUtils()
    {}

    public static void closeQuietly(AutoCloseable c)
    {
        try
        {
            if (c != null)
                c.close();
        }
        catch (Exception e)
        {
            logger.warn("Failed closing {}", c, e);
        }
    }
}
