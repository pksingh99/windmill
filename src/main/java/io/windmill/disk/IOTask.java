package io.windmill.disk;

import java.io.IOException;

@FunctionalInterface
public interface IOTask<O>
{
    O compute() throws IOException;
}
