package cs451.Broadcasts;
import java.io.IOException;
import java.net.InetAddress;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import cs451.Helper;
import cs451.Host;
import cs451.Parser;
import cs451.Links.PerfectLink;
import cs451.Links.StubbornLinkWithAck;


//TODO could implemenent this entire logic within uniform reliable broadcast within a function called bestEffortBroadcast.
public class BestEffortBroadcast extends Broadcast {
    ConcurrentLinkedQueue<String> log;
    Parser parser;
    List<Host> hosts;
    PerfectLink perfectLink; 
    UniformReliableBroadcast uniformReliableBroadcast;
    boolean terminated;
    boolean keepLogs;
  
    public BestEffortBroadcast(Parser parser, UniformReliableBroadcast caller, boolean keepLogs) {
        log =  new ConcurrentLinkedQueue<String>();
        hosts = parser.hosts();
        perfectLink = new PerfectLink(parser, this);
        this.parser = parser;
        uniformReliableBroadcast = caller;
        this.keepLogs = keepLogs;
    }

    public boolean broadcast(String msg_uid, String msg) throws IOException {
        if(keepLogs) {log.add("beb b" + Helper.getSeqNumFromMessageUid(msg_uid));};
        for(Host h : hosts) {
            perfectLink.send(h, msg_uid, msg);
        }
        return true;
    }
    public boolean deliver(String rawData) throws IOException {
        if(uniformReliableBroadcast != null) {
            uniformReliableBroadcast.deliver(rawData);
        }
        if(keepLogs) {
             log.add("beb d " + Helper.getProcIdFromMessageUid(Helper.getMsgUid(rawData)) 
                 + " " + Helper.getSeqNumFromMessageUid(Helper.getMsgUid(rawData)));
        }
       
        return true;
    }

    public PerfectLink getPerfectLink() {
        return perfectLink;
    }

    public StubbornLinkWithAck getStubbornLink() {
        return perfectLink.getStubbornLink();
    }

    public ConcurrentLinkedQueue<String> getLogs() {
        return log;
    }

    public boolean terminate() {
        terminated = true; 
        return perfectLink.terminate();
    }

    public boolean finished() {
        return true;
    }
}