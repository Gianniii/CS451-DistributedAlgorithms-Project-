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
  
    public BestEffortBroadcast(Parser parser, UniformReliableBroadcast caller) {
        log =  new ConcurrentLinkedQueue<String>();
        hosts = parser.hosts();
        perfectLink = new PerfectLink(parser, this);
        this.parser = parser;
        uniformReliableBroadcast = caller;
    }

    public boolean broadcast(String msg_uid, String msg) throws IOException {
        //log.add("beb b" + Helper.getSeqNumFromMessageUid(msg_uid));
        //must send in parallel since broadcast may never terminate if a host crashes
        //need to use an execturoService otherwise could create too much congestion on network if all hosts try sending 100 messages concurrently
        ExecutorService executorService = Executors.newFixedThreadPool(parser.hosts().size()/2 + 1);
        for(Host h : hosts) {    
            Thread t1 = new Thread(new Runnable() {
                @Override //Treat received packet in new thread so i can continue listening 
                public void run() {
                    try {
                        perfectLink.send(h, msg_uid, msg);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            });    
            executorService.execute(t1);
        }
        executorService.shutdown();
        /**for(Host h : hosts) {    
            Thread t1 = new Thread(new Runnable() {
                @Override //Treat received packet in new thread so i can continue listening 
                public void run() {
                    try {
                        perfectLink.send(h, msg_uid, msg);
                        
                    } catch (IOException e) {
                        // TODO Auto-generated catch block
                        e.printStackTrace();
                    }
                }
            });  
            t1.start();
        }*/
    
        return true;
    }
    public boolean deliver(String rawData) throws IOException {
        if(uniformReliableBroadcast != null) {
            uniformReliableBroadcast.deliver(rawData);
        }
        //log.add("beb d " + Helper.getProcIdFromMessageUid(Helper.getMsgUid(rawData)) 
        //         + " " + Helper.getSeqNumFromMessageUid(Helper.getMsgUid(rawData)));
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