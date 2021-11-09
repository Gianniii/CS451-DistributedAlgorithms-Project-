package cs451.Links;

import java.io.IOException;
import java.net.InetAddress;

import cs451.Host;

public abstract class Link {
    abstract public boolean send(Host dst, String uid, String msg) throws IOException;
    abstract public boolean deliver(String rawData) throws IOException;
}
