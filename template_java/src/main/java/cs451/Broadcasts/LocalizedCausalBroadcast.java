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
    
    //TODO init vector clock with 0's everywhere
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
    public boolean broadcast(String msg_uid, String msg) throws IOException {
        //System.out.println("FIFO Broadcast: " + msg_uid);
        //TODO extend message with VC = [p1,p2,....,pn] 
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

        //KEEP IN MIND THAT LIKE FOR FIFO WILL HAVE TO LOOP AGAIN IF I SUCCESSFULLY DELIVERED SOMETHING!!
        //probably wont have to use FIFO deliver is implement correctly with vector clocks
        return FIFODeliver(rawData);
    }

    public Boolean FIFODeliver(String rawData) {
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
    
    //TODO implement methods for to manipulate vector clock => DONE NEED TESTING
    //TODO implement methods to read the new configs => DONE NEED TESTING
    //TODO implement classic causal broadcast
    //TODO modify algorithm to be LOCALIZED causal broadcast intstead of classic causal broadcast
        //Probably just means not setting and always leaving at 0 the values VC[pk] for all pk im not dependent on... i.e only using the VC for
        //processes im causaly affected by.
        //PROBLEM then it wouldnt be FIFO... sol: for processes im not affected by only compare VC[msgOriginalSrc] > VCm[msgOriginalSrc] !! should work..

    /**
     * Builds string representing the VC of the form [0, p1,p2,...,pn] with n being the number of hosts
     * @return String for VC of the form  [0, p1,p2,...,pn]
     */
    public String encodeVC() {
        StringBuilder VCBuilder = new StringBuilder();
        VCBuilder.append("[");
        for(int i = 1; i < hosts.size(); i++) {
            VCBuilder.append( String.valueOf(VC[i]) + ",");
        }
        VCBuilder.append(String.valueOf(hosts.size()));
        VCBuilder.append("]");
        return VCBuilder.toString();
    }

    /**
     * Decodes and returns a int[] representation of the vector clock in the rawData
     * @param rawData
     * @return vector clock contained in the received rawData
     */
    public int[] decodeMsgVC(String rawData) {
        int index1 = rawData.indexOf("[");
        int index2 = rawData.indexOf("]");
        String[] msgVCString = rawData.substring(index1+1, index2).split(",");
        int[] decodedMsgVC = new int[hosts.size()+1];
        int i = 0;
        for(String vc : msgVCString) {
            decodedMsgVC[i+1] = Integer.valueOf(vc);
            i++;
        }
        return decodedMsgVC;
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

}
