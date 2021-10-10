package cs451.Links;

import java.net.InetAddress;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.util.LinkedList;
import java.lang.IllegalArgumentException;

//implement a stubborn link that stops sending once it received an ack
public class StubbornLink extends Link{
    Link perfectLink;
    Link fairLossLink; 
    Collection deliveredMessages;
    HashMap<String, Boolean> sentUidAck;

    public StubbornLink(Link caller) {
        super();
        if(caller != null)
            perfectLink = caller;
        else {
            throw new IllegalArgumentException("caller is null");
        }
        this.fairLossLink = new FairLossLink(this);
        
    }


    //maybe can pack all into DATA JUST SEND "DATA" instead of uid seperateÿ
    public boolean send(InetAddress destIp, int port, String uid, String msg) throws IOException{
        //Note could also use another fair loss link here instead of udp?? no point tho right?
        //need to put a loop with some kind of ack so it stops sending eventually ack could be implemented somehow with UDP msgs..
        //like when another stubborn link receives! is sendwhat would garantee  back a UDP ack... but i received that ack?
        //well the garante would be that i just keep delivering UNTIL I DO!!! POGGGG

        //periodically send packet until we receive and ack for it
        try {
            while(!sentUidAck.getOrDefault(uid, false)) {
                fairLossLink.send(destIp, port, uid, msg);
            }
            Thread.sleep(1000);
        } catch(InterruptedException ex) {}
       
        return true;
    }
    
    //do not call perfectlink more then once
    public boolean deliver(String msg) {
        //No message is delivered more then once 
        //uid = getUid(msg)
        //src = getSrc(msg) //need and IP and port for udb packet
        String uid = null;

        //if received msg is an ACK then we set its delivered value to true and return
        

        //if is not an ack then we already delivered it if we didnt we deliver and add it to a delivered collection
        //              if we did deliver it before then we send out another ack udp msg to the src
        if(!sentUidAck.containsKey(uid)) {
            perfectLink.deliver(msg);
            sentUidAck.put(uid, false);
            //ack the delivery to src
        } else {
            //send UDP packet to ack again
        }

        return true;
    }
}
