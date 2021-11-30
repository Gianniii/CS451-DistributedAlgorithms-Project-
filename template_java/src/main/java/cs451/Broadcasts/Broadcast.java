package cs451.Broadcasts;

import java.io.IOException;
import java.util.concurrent.ConcurrentLinkedQueue;

import cs451.Links.StubbornLinkWithAck;

public abstract class Broadcast {
    abstract public boolean broadcast(String msgUid, String msg) throws IOException;
    abstract public boolean deliver(String rawData) throws IOException;
    abstract public ConcurrentLinkedQueue<String> getLogs();
    abstract public StubbornLinkWithAck getStubbornLink();
    abstract public boolean terminate(); //stop all packet processing
    abstract public boolean finished();
}

