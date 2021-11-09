package cs451.Broadcasts;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

import cs451.Host;
import cs451.Parser;
import cs451.Links.Helper;

public class FIFOBroadcast extends Broadcast{
    ConcurrentLinkedQueue<String> log;
    UniformReliableBroadcast uniformReliableBroadcast;
    List<Host> hosts; //list of all processes
    Set<String> deliveredUid; // messages i have already delivered
    Set<String> pending; //msgs i have seen & bebBroadcast but not delivered yet, Strings contain rawData i.e (msg_uid + msg) 
    int[] next;
    Parser parser;
    
    public FIFOBroadcast(Parser parser) {
        this.parser = parser;
        log =  new ConcurrentLinkedQueue<String>();
        uniformReliableBroadcast = new UniformReliableBroadcast(parser, this);
        hosts = parser.hosts();
        deliveredUid = Collections.newSetFromMap(new ConcurrentHashMap<String, Boolean>());
        pending = Collections.newSetFromMap(new ConcurrentHashMap<String, Boolean>());
        next = new int[parser.hosts().size()];
    }

    public boolean broadcast(String msg_uid, String msg) throws IOException {
        uniformReliableBroadcast.broadcast(msg_uid, msg);
        log.add("b " + Helper.getSeqNumFromMessageUid(msg_uid));
        return true;

    }
    public boolean deliver(String rawData) throws IOException {
        //TODO think about how what datastructure to use for pending
        pending.add(rawData);
        boolean iterateAgain = true;
        
        //TODO use better datastructure for pending, where store rawData by procId so only need to check if i can deliver messages
        //of same procId as the rawData i received(only iterate over rData in pending[procId])
        while(iterateAgain){
        iterateAgain = false;
            for(String rData: pending) {
                String msgUid = Helper.getMsg(rData);
                int procId = Integer.parseInt(Helper.getProcIdFromMessageUid(msgUid));
                String seqNum = Helper.getSeqNumFromMessageUid(Helper.getMsgUid(rawData));
                if(next[procId] == Integer.parseInt(seqNum)) {
                    pending.remove(rData);
                    next[procId]++;
                    iterateAgain = true; //check if can send another
                    //FIFODeliver
                    log.add("d " + Helper.getProcIdFromMessageUid(Helper.getMsgUid(rawData)) 
                    + " " + Helper.getSeqNumFromMessageUid(Helper.getMsgUid(rawData)));
                }
            }  
        }
        return true;
    }

    public UniformReliableBroadcast getUniformReliableBroadcast() {
        return uniformReliableBroadcast;
    }

    public ConcurrentLinkedQueue<String> getLogs() {
        return log;
    }
    
}
