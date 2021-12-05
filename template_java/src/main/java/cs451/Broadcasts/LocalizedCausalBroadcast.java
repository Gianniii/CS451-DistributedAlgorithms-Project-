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


public class LocalizedCausalBroadcast extends Broadcast{
    ConcurrentLinkedQueue<String> log;
    UniformReliableBroadcast uniformReliableBroadcast;
    List<Host> hosts; //list of all processes
    int[] next;
    Parser parser;
    boolean terminated = false;
    Set<String> myCausallyAffectingHosts;
    int[] VC; //vector clock (of size hosts.size + 1), since host ID's start at 1. Contains {0, p1, p2,..,pn} 
    //where pi is the number of delivered messages from host pi
    Set<String> pending; //msgs i have seen & broadcast but not delivered yet
    
    public LocalizedCausalBroadcast(Parser parser) {
        this.parser = parser;
        log =  new ConcurrentLinkedQueue<String>();
        uniformReliableBroadcast = new UniformReliableBroadcast(parser, this, false);
        hosts = parser.hosts();
        pending = Collections.newSetFromMap(new ConcurrentHashMap<String, Boolean>());
        myCausallyAffectingHosts = parser.getProcessesAffectingMe();
        //System.out.println("myCausallyAffectingHosts: " + myCausallyAffectingHosts);
        VC = new int[hosts.size()+1]; //Keeps track of how many msgs i have delivered from each host
        //(there is no Host with id 0, so the first elt will not be used
    }

    /**
     * Localized Causal broadcast a message
     */
    public boolean broadcast(String msgUid, String msg) throws IOException {
        //System.out.println("FIFO Broadcast: " + msg_uid);
        String newMsg;
        synchronized(this) {
            log.add("b " + Helper.getSeqNumFromMessageUid(msgUid)); 
            //Immediatly deliver message
            log.add("d " + Helper.getProcIdFromMessageUid(msgUid) + " " + Helper.getSeqNumFromMessageUid(msgUid));

            //Encode my current vector clock into msg content and broadcast it
            String VCm = getEncodedVCForDependencies();
            newMsg = Helper.encodeVectorClockInMsg(VCm, msg);
            //System.out.println("Broadcasting :" + msgUid + newMsg);
            VC[parser.myId()]++;
        }
            uniformReliableBroadcast.broadcast(msgUid, newMsg);
            
        
        return true;
    }
    
    /**
     * Receives rawData of the form [msgUid+msg]
     * Lcausal deliver
     */
    public boolean deliver(String rawData) throws IOException {
        //probably wont have to use FIFO deliver is implement correctly with vector clocks
        //System.out.println("CausalReceived: " + rawData);
        //System.out.println("MyVectorClock :" + getEncodedVC());
        String originalSrcId = Helper.getProcIdFromMessageUid(Helper.getMsgUid(rawData));
        //System.out.println("OgSrcID : " + originalSrcId + " myId: " + String.valueOf(parser.myId()));
        //ignore my own broadcasts
        if(originalSrcId.equals(String.valueOf(parser.myId()))) {
            return true;
        };

        synchronized(this) {
            pending.add(rawData);
            deliverPending();
        }
        return true; 
    }

    /**
     * If can, delivers messages contained if pending
     * @return true
     */
    private boolean deliverPending() {
        boolean iterateAgain = true;
        while(iterateAgain && !terminated){
            iterateAgain = false;
            for(String rawData : pending) {
                if(canDeliverCausalBroadcast(rawData)) {
                    //System.out.println("MyVectorClock :" + getEncodedVC());
                    //System.out.println("Delivers rawData: " + rawData);
                    pending.remove(rawData);
                    iterateAgain = true; //can iterate again to check if this deliver allows to deliver any other pending messages
                    //deliver
                    log.add("d " + Helper.getProcIdFromMessageUid(Helper.getMsgUid(rawData)) 
                    + " " + Helper.getSeqNumFromMessageUid(Helper.getMsgUid(rawData)));
                    String originalSrcId = Helper.getProcIdFromMessageUid(Helper.getMsgUid(rawData));
                    VC[Integer.valueOf(originalSrcId)]++;
                    
                }
            }
        }
        //System.out.println("finished Deliver");
        return true;
    }

    /**
     * Implements deliver criteria for standard causal broadcast
     * @param rawData
     * @return
     */
    private boolean canDeliverCausalBroadcast(String rawData) {
        boolean canDeliver = true;
        int[] VCmsg = decodeVC(rawData);

        for(int i = 1; i < hosts.size()+1; i ++){ //(start with i = 1 because index 0 is not used since hostID's start at 1)
            if(VC[i] < VCmsg[i]) {canDeliver = false;};
        }

        return canDeliver;
    }

    /**
     * Implements deliver criteria for Localized Causal Broadcast
     * @return
     */
    private boolean canDeliverForLocalizedCausalBroadcast(String rawData){
        boolean canDeliver = true;
        int[] VCmsg = decodeVC(rawData);
        String originalSrcId = Helper.getProcIdFromMessageUid(Helper.getMsgUid(rawData));
        int originalSrcIdInt = Integer.valueOf(originalSrcId);
        //if i am causally affected by src host then check all the vector clocks
  
        if(myCausallyAffectingHosts!= null && myCausallyAffectingHosts.contains(originalSrcId)) {
            //System.out.println("im causally affected by this host for raw data " + rawData);
            for(int i = 1; i < hosts.size()+1; i ++){ //(start with i = 1 because index 0 is not used since hostID's start at 1)
                if(VC[i] < VCmsg[i]) {canDeliver = false;};
            }
        } 
        //if i am not causally effected by the src host of the msg then only need to check VC[originalSenderId]
        //to insure FIFO ordering
        else if(VC[originalSrcIdInt] < VCmsg[originalSrcIdInt]) {
            canDeliver = false;
        }
        

        return canDeliver;

    }



    /**
     * Build a string VC, This VC is identical to my own VC for the indexes of 
     * processes i am affected by, else 0 
     * 
     * @return The returned VC represents my VC with 0 at indexes of hosts i am not affected by
     * and for hosts i am Affected it contains the current value in my VC
     */
    private String getEncodedVCForDependencies() {
        StringBuilder VCBuilder = new StringBuilder();
        VCBuilder.append("[");
        //always dependent on myself(this ensure FIFO ordering)
        for(int i = 0; i < hosts.size(); i++) {
            if(myCausallyAffectingHosts.contains(String.valueOf(i)) || i == parser.myId()){
                VCBuilder.append( String.valueOf(VC[i]) + ",");
            } else {
                VCBuilder.append("0" + ",");
            }
            
        }
        int i = hosts.size();
        if(myCausallyAffectingHosts.contains(String.valueOf(i)) || i == parser.myId()){
            VCBuilder.append(String.valueOf(VC[i]));
        } else {
            VCBuilder.append("0");
        }
        VCBuilder.append("]");
        return VCBuilder.toString();
    }


    /**
     * Builds string representing the VC of the form [0, p1,p2,...,pn] with n being the number of hosts
     * @return String for VC of the form  [0, p1,p2,...,pn]
     */
    private String getEncodedVC() {
        StringBuilder VCBuilder = new StringBuilder();
        VCBuilder.append("[");
        for(int i = 0; i < hosts.size(); i++) {
            VCBuilder.append( String.valueOf(VC[i]) + ",");
        }
        VCBuilder.append(String.valueOf(VC[hosts.size()]));
        VCBuilder.append("]");
        return VCBuilder.toString();
    }

    /**
     * Decodes and returns a int[] representation of the vector clock in the rawData
     * @param rawData
     * @return vector clock contained in the received rawData
     */
    private int[] decodeVC(String rawData) {
        int index1 = rawData.indexOf("[");
        int index2 = rawData.indexOf("]");
        String[] msgVCString = rawData.substring(index1+1, index2).split(",");
        int[] decodedMsgVC = new int[hosts.size()+1];
        int i = 0;
        for(String vc : msgVCString) {
            decodedMsgVC[i] = Integer.valueOf(vc);
            i++;
        }
        return decodedMsgVC;
    }


    /**
     * Extract msg from rawData
     * @param rawData
     * @return portion of rawData containing the msg
     */
    private String getMsg(String rawData) {
        int index = rawData.indexOf("]");
        return rawData.substring(index+1);
    }
  

    /**
     * Reads value at given index for given vector clock in String representation
     * @param index
     * @param VC //String of format "p1,p2,...,pn" where pi represent integer values
     * @return integer value of pi where i = index
     */
    public int readVCi(int index, String VC) {
        return Integer.valueOf(VC.split(",")[index]);
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
