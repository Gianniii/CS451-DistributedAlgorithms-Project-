package cs451.Broadcasts;

import java.io.IOException;
import java.net.InetAddress;

public abstract class Broadcast {
    abstract public boolean broadcast(String msg_uid, String msg) throws IOException;
    abstract public boolean deliver(String rawData) throws IOException;
}

