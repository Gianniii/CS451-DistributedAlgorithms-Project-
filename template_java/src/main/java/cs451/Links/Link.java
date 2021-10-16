package cs451.Links;

import java.io.IOException;
import java.net.InetAddress;

public abstract class Link {
    abstract public boolean send(InetAddress destIp, int port, String uid, String msg) throws IOException;
    abstract public boolean deliver(String msg) throws IOException;
}
