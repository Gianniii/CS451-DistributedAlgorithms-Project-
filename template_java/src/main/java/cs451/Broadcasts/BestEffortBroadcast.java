package cs451.Broadcasts;
import java.io.IOException;
import java.net.InetAddress;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import cs451.Host;
import cs451.Parser;
import cs451.Links.PerfectLink;


//TODO could implemenent this entire logic within uniform reliable broadcast within a function called bestEffortBroadcast.
public class BestEffortBroadcast extends Broadcast {

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
        for(Host h : hosts) {     
            perfectLink.send(InetAddress.getByName(h.getIp()), h.getPort(), msg_uid, msg);
        }
        return true;
    }
    public boolean deliver(String rawData) throws IOException {
       uniformReliableBroadcast.deliver(rawData);
       return true;
    }

    public PerfectLink getPerfectLink() {
        return perfectLink;
    }
}