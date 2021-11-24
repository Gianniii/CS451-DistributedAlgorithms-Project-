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
    
    int[] VC; //vector clock (of size hosts.size + 1), since host ID's start at 1.
    Set<String> pending; //msgs i have seen & broadcast but not delivered yet, String contain .. 
    
    public LocalizedCausalBroadcast(Parser parser) {
        this.parser = parser;
        log =  new ConcurrentLinkedQueue<String>();
        uniformReliableBroadcast = new UniformReliableBroadcast(parser, this);
        hosts = parser.hosts();
        pending = Collections.newSetFromMap(new ConcurrentHashMap<String, Boolean>());
        //COULD REPRESENT MY VC LIKE I DID WITH NEXT!! WOULD JUST NEED TO ENCODE WHEN BROADCASTING AND DECODE WHEN RECEIVING!! IS IT WORTH IT ?
        //COULD BE EASIER TO UPDATE VALUES 
        VC = new int[hosts.size()+1]; //there is no Host with id 0, so the first elt will not be used
    }

    /**
     * FIFO broadcast a message
     */
    public boolean broadcast(String msgUid, String msg) throws IOException {
        //System.out.println("FIFO Broadcast: " + msg_uid);
        //TODO extend message with VC = [p1,p2,....,pn] 
        log.add("b " + Helper.getSeqNumFromMessageUid(msgUid)); 
        log.add("d " + Helper.getProcIdFromMessageUid(msgUid) + " " + Helper.getSeqNumFromMessageUid(msgUid));//deliver immediatly

        String VCm = getEncodedVC();
        String newMsg = Helper.encodeVectorClockInMsg(VCm, msg);
        uniformReliableBroadcast.broadcast(msgUid, newMsg);
      
        System.out.println("myID: " + parser.myId());
        VC[parser.myId()]++;

        return true;
    }
    
    /**
     * Receives rawData of the form [msgUid+msg]
     * adds rawData to pending and if can delivers message in 
     * FIFO order
     */
    public boolean deliver(String rawData) throws IOException {
        //KEEP IN MIND THAT LIKE FOR FIFO WILL HAVE TO LOOP AGAIN IF I SUCCESSFULLY DELIVERED SOMETHING!!
        //probably wont have to use FIFO deliver is implement correctly with vector clocks
        System.out.println("CausalReceived: " + rawData);
        String originalSrcId = Helper.getProcIdFromMessageUid(Helper.getMsgUid(rawData));
        System.out.println("OgSrcID : " + originalSrcId + " myId: " + String.valueOf(parser.myId()));
        if(originalSrcId.equals(String.valueOf(parser.myId()))) {
            System.out.println("skips this message");
            return true;
        }; //ignore my own broadcasts
        pending.add(rawData);
        return deliverPending();
    }

    private boolean deliverPending() {
        boolean iterateAgain = true;
        while(iterateAgain && !terminated){
            iterateAgain = false;
            for(String rawData : pending) {
                if(canDeliverCausalBroadcast(rawData)) {
                    pending.remove(rawData);
                    iterateAgain = true; 
                    //deliver
                    log.add("d " + Helper.getProcIdFromMessageUid(Helper.getMsgUid(rawData)) 
                    + " " + Helper.getSeqNumFromMessageUid(Helper.getMsgUid(rawData)));
                    String originalSrcId = Helper.getProcIdFromMessageUid(Helper.getMsgUid(rawData));
                    VC[Integer.valueOf(originalSrcId)]++;
                }
            }
        }
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

        for(int i = 1; i < hosts.size()+1; i ++){
            if(VC[i] < VCmsg[i]) {canDeliver = false;};
        }

        return canDeliver;
    }

    /**
     * Implements deliver criteria for localizedCausalBroadcast
     * @return
     */
    private boolean canDeliverForLocalizedCausalBroadcast(rawData){
        boolean canDeliver = true;
        int[] VCmsg = decodeVC(rawData);
        
        for(int i = 1; i < hosts.size()+1; i ++){
            if(VC[i] < VCmsg[i]) {canDeliver = false;};
        }

        return canDeliver;

    }
    

    //TODO implement methods to read the new configs (still gotta see if it works)
    //TODO modify algorithm to be LOCALIZED causal broadcast intstead of classic causal broadcast
      //for processes im not affected by only compare VC[msgOriginalSrc] > VCm[msgOriginalSrc] !! should work..

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
     * 
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
