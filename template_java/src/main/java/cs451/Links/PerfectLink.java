package cs451.Links;
import java.net.InetAddress;
import java.io.IOException;
import java.util.Set;

import cs451.Host;
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
    Host dst;
    boolean terminated;

    public PerfectLink(Parser parser, BestEffortBroadcast caller) {
        stubbornLinkWithAck = new StubbornLinkWithAck(this, parser);
        deliveredUid = Collections.newSetFromMap(new ConcurrentHashMap<String, Boolean>());
        log =  new ConcurrentLinkedQueue<String>();
        this.caller = caller;
    }

    public boolean send(Host h, String msg_uid, String msg) throws IOException {
        dst = h;
        stubbornLinkWithAck.send(dst, msg_uid, msg);
        //log.add("b " + Helper.getSeqNumFromMessageUid(msg_uid));
        return true;
    }


    public boolean deliver(String rawData) throws IOException{
        //do not deliver same message from same sender more then once
        //System.out.println("perfect link deliver: " + rawData);
        String msgUid = Helper.getMsgUid(rawData);
        String senderId = Helper.getSenderId(rawData);

        if(!deliveredUid.contains(senderId + msgUid)){
            //add msgUid to delivered messages set 
            deliveredUid.add(senderId + msgUid); 
            /**log.add("d " + Helper.getProcIdFromMessageUid(Helper.getMsgUid(rawData)) 
                + " " + Helper.getSeqNumFromMessageUid(Helper.getMsgUid(rawData)));**/
            caller.deliver(rawData);
        }

        return true;
    }

    public StubbornLinkWithAck getStubbornLink() {
        return stubbornLinkWithAck;
    }

    public ConcurrentLinkedQueue<String> getLogs() {
        return log;
    }

    public boolean terminate() {
        terminated = true; 
        return stubbornLinkWithAck.terminate();
    }
    
}
