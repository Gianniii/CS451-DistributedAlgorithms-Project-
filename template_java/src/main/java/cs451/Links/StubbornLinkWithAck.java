package cs451.Links;

import java.net.InetAddress;
import java.io.IOException;
import java.lang.IllegalArgumentException;
import cs451.Parser;
import cs451.Host;
import java.lang.Integer;
import java.util.HashSet;
import java.util.Set;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.util.concurrent.ConcurrentHashMap;
import java.util.Collections;
import java.util.Random;

//implement a stubborn link that stops sending a message once it receives an ack for it
public class StubbornLinkWithAck extends Link {
    public static final String ACK = "ACK";
    Link perfectLink;
    Set<String> ackedMuid; //contains msg_uid as key if it was delivered, the value indicates if the msg was acked
    Parser parser;

    public StubbornLinkWithAck(Link caller, Parser parser) {
        super();
        
        if(parser != null)
            this.parser = parser;
        else {
            throw new IllegalArgumentException("parser is null");
        }

        if(caller != null)
            perfectLink = caller;
        else {
            throw new IllegalArgumentException("caller is null");
        }
        ackedMuid = Collections.newSetFromMap(new ConcurrentHashMap<String, Boolean>());
    }

    public boolean send(Host h, String msgUid, String msg) throws IOException{
        //send until message gets acked
        Random rand = new Random();
        //Block until the message is acked
        String myIdWithMsgUid = Helper.extendWithSenderId(h.getId(), msgUid);
        System.out.println("Stubborn Send :"+ "waiting for " + myIdWithMsgUid);
        while(!ackedMuid.contains(myIdWithMsgUid)) { 
            //System.out.println(Helper.getProcIdFromMessageUid(msg_uid) + "retransmitting" + msg_uid);
            String rawData = Helper.addSenderIdAndMsgUidToMsg(parser.myId(), msgUid, msg);
            byte buf[] = rawData.getBytes();
            //System.out.println("Stubborn sending raw: " + rawData + "to port :" + h.getPort());
            sendUDP(h, buf); //UDP is used as a fair loss link
            try {
                int sleepTime = rand.nextInt(500);
                Thread.sleep(sleepTime);
            } catch (InterruptedException e) {
                System.out.println("Sleep interrupted.");
                //return false;
            }
            
        }
        return true;
    }
    
    public boolean deliver(String rawData) throws IOException {
        String msg = Helper.getMsg(rawData);
        String msg_uid = Helper.getMsgUid(rawData);
        //System.out.println("Stubborn Deliver raw data: " + rawData);
        //if received message is an ack then add msg_uid to set of acked messages
        //so that this process will stop resending the same message
        if(msg.equals(ACK)) {
            String senderIdAndMsgUid = Helper.extendWithSenderId(Integer.parseInt(Helper.getSenderId(rawData)), msg_uid);
            ackedMuid.add(senderIdAndMsgUid);
            return true;
        } 

        //deliver and ack
        perfectLink.deliver(rawData);
        ackMsg(rawData);
        
        return true;
    }
 
    public boolean ackMsg(String rawData) throws IOException{

        int senderId = Integer.parseInt(Helper.getSenderId(rawData));

        Host host = parser.getHost(senderId);
        if(host == null) {
            return false;
        }
        String msg_uid = Helper.getMsgUid(rawData);
        String msg = Helper.extendWithSenderId(parser.myId(), Helper.appendMsg(msg_uid, ACK));
        byte buf[] = msg.getBytes();
        sendUDP(host, buf);
        return true;
    }

    private Boolean sendUDP(Host dstH, byte buf[]) throws IOException {
        DatagramPacket DpSend = new DatagramPacket(buf, buf.length, InetAddress.getByName(dstH.getIp()), dstH.getPort());
        DatagramSocket ds = new DatagramSocket();
        ds.send(DpSend);
        ds.close();
        return true;
    }
}
