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

    public boolean send(InetAddress destIp, int port, String msg_uid, String msg) throws IOException{
        //send until message gets acked
        Random rand = new Random();
        while(!ackedMuid.contains(msg_uid)) {
            //System.out.println(Helper.getProcIdFromMessageUid(msg_uid) + "retransmitting" + msg_uid);
            byte buf[] = Helper.appendMsg(msg_uid, msg).getBytes();
            sendUDP(destIp, port, buf); //UDP is used as a fair loss link
            try {
                int sleepTime = rand.nextInt(500);
                Thread.sleep(sleepTime);
            } catch (InterruptedException e) {
                System.out.println("Sleep interrupted.");
            }
            
        }
        
        return true;
    }
    
    public boolean deliver(String rawData) throws IOException {
        String msg = Helper.getMsg(rawData);
        String msg_uid = Helper.getMsgUid(rawData);

        //if received message is an ack then add msg_uid to set of acked messages
        //so that this process will stop resending the same message
        if(msg.equals(ACK)) {
            ackedMuid.add(msg_uid);
            return true;
        } 

        //deliver and ack
        perfectLink.deliver(rawData);
        ackMsg(msg_uid);
        
        return true;
    }
 
    public boolean ackMsg(String msg_uid) throws IOException{

        int hostUid = Integer.parseInt(Helper.getProcIdFromMessageUid(msg_uid));

        Host host = parser.getHost(hostUid);
        if(host == null) {
            return false;
        }
        String msg = Helper.appendMsg(msg_uid, ACK);
        byte buf[] = msg.getBytes();
        sendUDP(InetAddress.getByName(host.getIp()), host.getPort(), buf);
        return true;
    }

    private Boolean sendUDP(InetAddress destIp, int port, byte buf[]) throws IOException {
        DatagramPacket DpSend = new DatagramPacket(buf, buf.length, destIp, port);
        DatagramSocket ds = new DatagramSocket();
        ds.send(DpSend);
        ds.close();
        return true;
    }
}
