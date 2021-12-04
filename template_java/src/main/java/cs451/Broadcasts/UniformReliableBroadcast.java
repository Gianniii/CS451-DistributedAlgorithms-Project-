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

public class UniformReliableBroadcast extends Broadcast {
    ConcurrentLinkedQueue<String> log;
    BestEffortBroadcast bestEffortBroadcast;
    List<Host> hosts; //list of all processes
    Set<String> deliveredUid; // messages i have already delivered
    Set<String> forward; //msgs i have seen & bebBroadcast but not delivered yet, Strings contain ("_" + msg_uid + msg) 
    Parser parser;
    ConcurrentHashMap<String, Set<Integer>> ackedMuid; //(msg_uid, Set processes that acked/retransmit it) 
    Broadcast caller;
    boolean terminated = false;
    boolean keepLogs;


    public UniformReliableBroadcast(Parser parser, Broadcast caller, boolean keepLogs) {
        //init
        this.caller = caller;
        this.parser = parser;
        log =  new ConcurrentLinkedQueue<String>();
        bestEffortBroadcast = new BestEffortBroadcast(parser, this, false);
        hosts = parser.hosts();
        ackedMuid = new ConcurrentHashMap<String, Set<Integer>>(); 
        deliveredUid = Collections.newSetFromMap(new ConcurrentHashMap<String, Boolean>());
        forward = Collections.newSetFromMap(new ConcurrentHashMap<String, Boolean>());
        this.keepLogs = keepLogs;
    }

    /**
     * URB broadcast
     * @param msgUid 
     * @param msg
     * @return always returns true
     */
    public boolean broadcast(String msgUid, String msg) throws IOException {
        //System.out.println("URB broadcast" + msgUid);
        forward.add(Helper.addSenderIdAndMsgUidToMsg(parser.myId(), msgUid, msg)); //add "_"+ msguid + msg to pending (add leading "_" to reuse Helper methods to unpack content)
        ackedMuid.put(msgUid,  Collections.newSetFromMap(new ConcurrentHashMap<Integer, Boolean>()));
        deliverIfCan(); 
        bestEffortBroadcast.broadcast(msgUid, msg);  
        if(keepLogs){log.add("b " + Helper.getSeqNumFromMessageUid(msgUid));};
        return true;

    }

    /**
     * Receives rawData of the form [senderId+msgUid+msg]
     * and URB delivers
     * @param rawData //data delivered from BEB
     * @return boolean
     */
    public boolean deliver(String rawData) throws IOException {
        //increment ack count in ackedMuid for Helper.getMsgUid(rawData)
        //System.out.println("URB receives raw data: " + rawData);
        String msgUid = Helper.getMsgUid(rawData);
        if(ackedMuid.get(msgUid)==null) {
            ackedMuid.put(msgUid, Collections.newSetFromMap(new ConcurrentHashMap<Integer, Boolean>()));
        }
        //ack the msg 
        ackedMuid.get(msgUid).add(Integer.parseInt(Helper.getSenderId(rawData))); 

        //forward if this message was not forwarded yet (can remove senderID as per the algorithms seen in class)
        //String msgUidAndData = Helper.removeSenderId(rawData);
        if(!forward.contains(rawData)){ //forward everything only once PB RAW DATA NOW CONTAINS SRC !! 
            forward.add(rawData);
            bestEffortBroadcast.broadcast(Helper.getMsgUid(rawData), Helper.getMsg(rawData)); 
        }
        deliverIfCan();
        return true;
    }

    /**
     * Delivers message if this message was acked by strictly 
     * more then half the processess
     */
    public boolean deliverIfCan() throws IOException {
        //Look in list of forward(pending) messages and send those who have been acked by more then half the hosts
        //and who have not been delivered before
        for(String rawData : forward) {
            String msgUid = Helper.getMsgUid(rawData);
            Set<Integer> acksForMsgUid = ackedMuid.getOrDefault(msgUid, Collections.newSetFromMap(new ConcurrentHashMap<Integer, Boolean>()));
            synchronized(this) {
                if(acksForMsgUid.size() > hosts.size()/2. && !deliveredUid.contains(msgUid)){
                    actuallyDeliver(rawData);
                }
            }
        }
        return true;
    }

    /**
     * Delivers data to the caller of this (URB)
     * @param rawData
     * @return true
     * @throws IOException
     */
    public boolean actuallyDeliver(String rawData) throws IOException {
        //add msgUid to delivered messages set 
        //System.out.println("URB Deliver :" + rawData);
        deliveredUid.add(Helper.getMsgUid(rawData)); 
        if(keepLogs) {
            log.add("d " + Helper.getProcIdFromMessageUid(Helper.getMsgUid(rawData)) 
            + " " + Helper.getSeqNumFromMessageUid(Helper.getMsgUid(rawData)));
        }
       
        if(caller != null){
            caller.deliver(rawData);
        }
        return true;
    }

    /**
     * getted for the lower link, bestEffortBroadcast
     * @return bestEffortBroadcast
     */
    public BestEffortBroadcast getBestEffortBroadcast() {
        return bestEffortBroadcast;
    }

    /**
     * Getter for the lower link, in my case StubbornLink
     * @return bestEffortBroadcast.getStubbornLink()
     */
    public StubbornLinkWithAck getStubbornLink() {
        return bestEffortBroadcast.getStubbornLink();
    }

    /**
     * Getter for my logs
     * @return log
     */
    public ConcurrentLinkedQueue<String> getLogs() {
        return log;
    }

    /**
     * Sets terminated boolean to true and calls terminate on lower link
     * @return bestEffortBroadcast.terminate()
     */
    public boolean terminate() {
        terminated = true; 
        return bestEffortBroadcast.terminate();
    }

}
