package cs451.Broadcasts;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

import cs451.Helper;
import cs451.Host;
import cs451.Parser;
import cs451.Links.StubbornLinkWithAck;

public class FIFOBroadcast extends Broadcast{
    ConcurrentLinkedQueue<String> log;
    UniformReliableBroadcast uniformReliableBroadcast;
    List<Host> hosts; //list of all processes
    Set<String> deliveredUid; // messages i have already delivered
    Set<String> pending; //msgs i have seen & bebBroadcast but not delivered yet, Strings contain rawData i.e (msg_uid + msg) 
    int[] next;
    Parser parser;
    boolean terminated = false;
    
    public FIFOBroadcast(Parser parser) {
        this.parser = parser;
        log =  new ConcurrentLinkedQueue<String>();
        uniformReliableBroadcast = new UniformReliableBroadcast(parser, this);
        hosts = parser.hosts();
        deliveredUid = Collections.newSetFromMap(new ConcurrentHashMap<String, Boolean>());
        pending = Collections.newSetFromMap(new ConcurrentHashMap<String, Boolean>());
        next = new int[parser.hosts().size()+1]; //there is no Host with id 0, so the first elt will not be used
    }

    /**
     * FIFO broadcast a message
     */
    public boolean broadcast(String msg_uid, String msg) throws IOException {
        //System.out.println("FIFO Broadcast: " + msg_uid);
        log.add("b " + Helper.getSeqNumFromMessageUid(msg_uid));
        uniformReliableBroadcast.broadcast(msg_uid, msg);
        return true;

    }
    
    /**
     * Receives rawData of the form [msgUid+msg]
     * adds rawData to pending and if can delivers message in 
     * FIFO order
     */
    public boolean deliver(String rawData) throws IOException {
        pending.add(Helper.getMsgUid(rawData) + Helper.getMsg(rawData));
        boolean iterateAgain = true;
        
        //TODO use better datastructure for pending, where store rawData by procId so only need to check if i can deliver messages
        //of same procId as the rawData i received(only iterate over rData in pending[procId])
        while(iterateAgain && !terminated){
        iterateAgain = false;
            for(String rData: pending) {
                String msgUid = Helper.getMsgUid(rawData);
                int originalSrcId = Integer.parseInt(Helper.getProcIdFromMessageUid(msgUid));
                String seqNum = Helper.getSeqNumFromMessageUid(Helper.getMsgUid(rawData));
                if(next[originalSrcId]+1 == Integer.parseInt(seqNum)) {
                    pending.remove(rData);
                    next[originalSrcId]++;
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

    public StubbornLinkWithAck getStubbornLink() {
        return uniformReliableBroadcast.getStubbornLink();
    }

    public ConcurrentLinkedQueue<String> getLogs() {
        return log;
    }

    public boolean terminate() {
        terminated = true; 
        return uniformReliableBroadcast.terminate();
    }
    
}
