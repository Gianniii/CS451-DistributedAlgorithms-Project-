package cs451.Links;

import java.net.InetAddress;
import java.io.IOException;
import java.lang.IllegalArgumentException;
import cs451.Parser;
import cs451.Helper;
import cs451.Host;
import java.lang.Integer;
import java.util.HashSet;
import java.util.Set;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.Collections;
import java.util.Random;

//implement a stubborn link that stops sending a message once it receives an ack for it
public class StubbornLinkWithAck extends Link {
    public static final String ACK = "ACK";
    Link perfectLink;
    Set<String> ackedMuid; //contains SenderId + msgUid as key if it was delivered, the value indicates if the msg was acked
    Parser parser;
    Boolean terminated = false;
    Set<String> sendingQueue;
    Thread sender;


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
        sendingQueue = Collections.newSetFromMap(new ConcurrentHashMap<String, Boolean>());

        sender = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    sendFromSendingQueue();
                } catch (IOException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            }
        });
        sender.start(); 
    }

    public boolean send(Host h, String msgUid, String msg) throws IOException{
        String rawData = Helper.addSenderIdAndMsgUidToMsg(parser.myId(), msgUid, msg);
        sendingQueue.add(setDestination(String.valueOf(h.getId()), rawData)); //add dst so know where to send it when try to transmit it
        return true;
    }


    private void sendFromSendingQueue() throws IOException{
        //send until message gets acked
        Random rand = new Random();
        while(true) {
            if(!sendingQueue.isEmpty() && !terminated) {
                for(String rawData : sendingQueue) {
                    Host dstHost = getDestination(rawData);
                    String msgUid = Helper.getMsgUid(rawData);
                    String dstIdWithMsgUid = Helper.extendWithSenderId(dstHost.getId(), msgUid);
                    //if have received an ack from dst for a given msguid remove it from queue else (re)transmit
                    if(ackedMuid.contains(dstIdWithMsgUid)) {
                        sendingQueue.remove(rawData);
                    } else {
                        String rawDataWithoutDst = removeDst(rawData);
                        byte buf[] = rawDataWithoutDst.getBytes();
                        sendUDP(dstHost, buf);
                    }
                    
                }
            }
            try {
                int sleepTime = rand.nextInt(getMaxRetransmitTime());
                //System.out.println("sleep time:" + sleepTime);
                Thread.sleep(sleepTime);
            } catch (InterruptedException e) {}
        }

    }
    
    public boolean deliver(String rawData) throws IOException {
        if(terminated) return true;

        String msg = Helper.getMsg(rawData);
        String msg_uid = Helper.getMsgUid(rawData);
        //System.out.println("Stubborn Deliver raw data: " + rawData);
        //if received message is an ack then add [senderId + msgUid] to set of acked messages
        if(msg.equals(ACK)) {
            String senderIdAndMsgUid = Helper.extendWithSenderId(Integer.parseInt(Helper.getSenderId(rawData)), msg_uid);
            ackedMuid.add(senderIdAndMsgUid);
            return true;
        } 

        //deliver and ack
        perfectLink.deliver(rawData);
        //System.out.println("perfect link finished delivering");
        ackMsg(rawData);
        
        return true;
    }
 
    public boolean ackMsg(String rawData) throws IOException{
        int senderId = Integer.parseInt(Helper.getSenderId(rawData));

        Host host = parser.getHost(senderId);
        if(host == null) {
            return false;
        }
        String msgUid = Helper.getMsgUid(rawData);
        String msg = Helper.extendWithSenderId(parser.myId(), Helper.appendMsg(msgUid, ACK));
        byte buf[] = msg.getBytes();
        //System.out.println("sending ack for raw " + rawData);
        sendUDP(host, buf);
        return true;
    }

    private Boolean sendUDP(Host dstH, byte buf[]) throws IOException {
        if(terminated) return true;
        DatagramPacket DpSend = new DatagramPacket(buf, buf.length, InetAddress.getByName(dstH.getIp()), dstH.getPort());
        DatagramSocket ds = new DatagramSocket();
        ds.send(DpSend);
        ds.close();
        return true;
    }

    public boolean stopReceivingAndSending() {
        terminated = true;
        return true;
    }

    public boolean terminate() {
       return stopReceivingAndSending();
    }

    public int getMaxRetransmitTime(){
        return (sendingQueue.size() < 10)? 10 : sendingQueue.size(); //use size of sending queue to estimate traffic
    }

    private String setDestination(String dst, String data) {
        return dst + "-" + data;
    }

    private Host getDestination(String rawData){
        String dstId = rawData.substring(0, rawData.indexOf("-"));
        return parser.getHost(Integer.valueOf(dstId));
    }

    private String removeDst(String rawData){
        int index = rawData.indexOf("-");
        return rawData.substring(index+1);
    }
}
