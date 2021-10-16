package cs451.Links;
import java.net.InetAddress;
import java.io.IOException;
import java.util.Set;
import cs451.Parser;
import java.util.HashSet;
import java.util.concurrent.ConcurrentLinkedQueue;
import cs451.Links.*;

public class PerfectLink extends Link{
    StubbornLinkWithAck stubbornLinkWithAck;
    Set<String> deliveredUid;
    ConcurrentLinkedQueue<String> log;

    public PerfectLink(Parser parser) {
        stubbornLinkWithAck = new StubbornLinkWithAck(this, parser);
        deliveredUid = new HashSet<String>();
        log =  new ConcurrentLinkedQueue<String>();
    }

    public boolean send(InetAddress destIp, int port, String msg_uid, String msg) throws IOException {
        stubbornLinkWithAck.send(destIp, port, msg_uid, msg);
        log.add("b " + Helper.getProcIdFromMessageUid(msg_uid) + " " + Helper.getSeqNumFromMessageUid(msg_uid));
        return true;
    }
    
    public boolean deliver(String rawData){
        //do not deliver same message more then once
        if(!deliveredUid.contains(Helper.getMsgUid(rawData))){
            System.out.println("Received message  ---- " + rawData + " ---- ...\n");
            log.add("d " + Helper.getProcIdFromMessageUid(Helper.getMsgUid(rawData)) 
                + " " + Helper.getSeqNumFromMessageUid(Helper.getMsgUid(rawData)));  //TODO SHOULDNT USE MSG DATA TO GET MSG SEQNUMBER!!!! 
                //NEED TO PACK SEQUENCE NUMBER IN THE RAW DATA!!! should not use processes should keep track
                //of a seq number and broadcast alongside the msg!! could be included in uid

            deliveredUid.add(Helper.getMsgUid(rawData));
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
