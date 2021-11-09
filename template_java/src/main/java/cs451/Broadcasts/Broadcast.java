package cs451.Broadcasts;

import java.io.IOException;
import java.util.concurrent.ConcurrentLinkedQueue;

public abstract class Broadcast {
    abstract public boolean broadcast(String msg_uid, String msg) throws IOException;
    abstract public boolean deliver(String rawData) throws IOException;
    abstract public ConcurrentLinkedQueue<String> getLogs();
}

