package cs451.Links;
import java.net.InetAddress;
import java.io.IOException;
import java.util.Set;
import cs451.Parser;
import cs451.Broadcasts.BestEffortBroadcast;
import cs451.Broadcasts.Broadcast;
import cs451.Broadcasts.UniformReliableBroadcast;

import java.util.HashSet;
import java.util.concurrent.ConcurrentLinkedQueue;
import cs451.Links.*;
import java.util.Collections;
import java.util.concurrent.ConcurrentHashMap;


public class PerfectLink extends Link{
    StubbornLinkWithAck stubbornLinkWithAck;
    Set<String> deliveredUid;
    ConcurrentLinkedQueue<String> log;
    Broadcast caller;

    public PerfectLink(Parser parser, BestEffortBroadcast caller) {
        stubbornLinkWithAck = new StubbornLinkWithAck(this, parser);
        deliveredUid = Collections.newSetFromMap(new ConcurrentHashMap<String, Boolean>());
        log =  new ConcurrentLinkedQueue<String>();
        this.caller = caller;
    }

    public boolean send(InetAddress destIp, int port, String msg_uid, String msg) throws IOException {
        stubbornLinkWithAck.send(destIp, port, msg_uid, msg);
        //log.add("b " + Helper.getSeqNumFromMessageUid(msg_uid));
        return true;
    }
    
    public boolean deliver(String rawData) throws IOException{
        //TODO: DELIVER TO ABOVE CHANNEL implement other channels
        //do not deliver same message more then once
        if(!deliveredUid.contains(Helper.getMsgUid(rawData))){
            //add msgUid to delivered messages set 
            deliveredUid.add(Helper.getMsgUid(rawData)); 
            //update log
            //log.add("d " + Helper.getProcIdFromMessageUid(Helper.getMsgUid(rawData)) 
            //    + " " + Helper.getSeqNumFromMessageUid(Helper.getMsgUid(rawData)));
            caller.deliver(rawData);
            
            return true;

        }
        return true;
    }

    public StubbornLinkWithAck getStubbornLink() {
        return stubbornLinkWithAck;
    }

    public ConcurrentLinkedQueue<String> getLogs() {
        return log;
    }
    
}
