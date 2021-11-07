package cs451.Broadcasts;

import java.io.IOException;
import java.net.InetAddress;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

import cs451.Host;
import cs451.Parser;
import cs451.Links.Helper;
import cs451.Links.PerfectLink;

public class UniformReliableBroadcast extends Broadcast {
    ConcurrentLinkedQueue<String> log;
    BestEffortBroadcast bestEffortBroadcast;
    List<Host> hosts; //list of all processes
    Set<String> deliveredUid; // messages i have already delivered
    Set<String> forward; //messages i have seen and bebBroadcast but not delivered yet, Strings contain rawData i.e (msg_uid + msg)
    Parser parser;
    ConcurrentHashMap<String, Set<Host>> ackedMuid; //(msg_uid, #acksReceivedforthis) //probably need a map
    
    public UniformReliableBroadcast(Parser parser) {
        //init
        this.parser = parser;
        log =  new ConcurrentLinkedQueue<String>();
        bestEffortBroadcast = new BestEffortBroadcast(parser, this);
        hosts = parser.hosts();
        //TODO could think a better datastructure instead of using three sets.
        //TODO implement cleanup of the sets.(garbage collection)
        ackedMuid = new ConcurrentHashMap<String, Set<Host>>(); 
        deliveredUid = Collections.newSetFromMap(new ConcurrentHashMap<String, Boolean>());
        forward = Collections.newSetFromMap(new ConcurrentHashMap<String, Boolean>());
    }

    public boolean broadcast(String msg_uid, String msg) throws IOException {
        String rawData = Helper.appendMsg(msg_uid, msg);
        forward.add(rawData); //add message to pending
        deliverIfCan(rawData); //check if can deliver message (TODO check if this is usefull)
        bestEffortBroadcast.broadcast(msg_uid, msg);
        log.add("b " + Helper.getSeqNumFromMessageUid(msg_uid));
        return true;

    }
    public boolean deliver(String rawData) throws IOException {
        //increment ack count in ackedMuid for Helper.getMsgUid(rawData)
        //do not deliver more then once 
        //for all elements in forward..... kinda confusing needa review
        String msg_uid = Helper.getMsgUid(rawData);
        String proc_id = Helper.getProcIdFromMessageUid(msg_uid);
        Host host = parser.getHost(Integer.parseInt(proc_id)); //process and host are equivalent in my implementation
        ackedMuid.get(msg_uid).add(host); //add host to list of hosts
        if(!forward.contains(rawData)){
            forward.add(rawData);
            bestEffortBroadcast.broadcast(Helper.getMsgUid(rawData), Helper.getMsg(rawData));
        }
        deliverIfCan(rawData);
        return true;
    }

    public boolean deliverIfCan(String rawData) {
        //TODO COMPLETE ME
        String msg_uid = Helper.getMsgUid(rawData);
        Set<Host> acksForMsgUid = ackedMuid.get(msg_uid);
        if(acksForMsgUid.size() > hosts.size()/2 && !deliveredUid.contains(msg_uid)){
            actuallyDeliver(rawData);
        }
        return true;
    }

    public boolean actuallyDeliver(String rawData) {
        //add msgUid to delivered messages set 
        deliveredUid.add(Helper.getMsgUid(rawData)); 
        //update log
        log.add("d " + Helper.getProcIdFromMessageUid(Helper.getMsgUid(rawData)) 
            + " " + Helper.getSeqNumFromMessageUid(Helper.getMsgUid(rawData)));
        return true;
    }

    public BestEffortBroadcast getBestEffortBroadcast() {
        return bestEffortBroadcast;
    }

    public ConcurrentLinkedQueue<String> getLogs() {
        return log;
    }
}
