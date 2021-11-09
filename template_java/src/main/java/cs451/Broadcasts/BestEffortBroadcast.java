package cs451.Broadcasts;
import java.io.IOException;
import java.net.InetAddress;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;

import cs451.Host;
import cs451.Parser;
import cs451.Links.Helper;
import cs451.Links.PerfectLink;


//TODO could implemenent this entire logic within uniform reliable broadcast within a function called bestEffortBroadcast.
public class BestEffortBroadcast extends Broadcast {
    ConcurrentLinkedQueue<String> log;
    Parser parser;
    List<Host> hosts;
    PerfectLink perfectLink; 
    UniformReliableBroadcast uniformReliableBroadcast;
    //TODO pass params for perfect link
    public BestEffortBroadcast(Parser parser, UniformReliableBroadcast caller) {
        hosts = parser.hosts();
        perfectLink = new PerfectLink(parser, this);
        this.parser = parser;
        if(caller != null)
            uniformReliableBroadcast = caller;
        else {
            throw new IllegalArgumentException("caller is null");
        }
    }

    public boolean broadcast(String msg_uid, String msg) throws IOException {
        //log.add("beb b" + Helper.getSeqNumFromMessageUid(msg_uid));
        System.out.println("BEB broadcast: " + msg_uid);
        for(Host h : hosts) {     
            perfectLink.send(InetAddress.getByName(h.getIp()), h.getPort(), msg_uid, msg);
        }
        return true;
    }
    public boolean deliver(String rawData) throws IOException {
        System.out.println("BEB deliver: " + Helper.getMsg(rawData));
        uniformReliableBroadcast.deliver(rawData);
        //log.add("beb d " + Helper.getProcIdFromMessageUid(Helper.getMsgUid(rawData)) 
        //             + " " + Helper.getSeqNumFromMessageUid(Helper.getMsgUid(rawData)));
        return true;
    }

    public PerfectLink getPerfectLink() {
        return perfectLink;
    }

    public ConcurrentLinkedQueue<String> getLogs() {
        return null;
    }
}