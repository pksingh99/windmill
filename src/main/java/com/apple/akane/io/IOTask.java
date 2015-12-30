package com.apple.akane.io;

import java.io.IOException;

@FunctionalInterface
public interface IOTask<O>
{
    O compute() throws IOException;
}
