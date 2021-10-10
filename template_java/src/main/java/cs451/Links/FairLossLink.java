package cs451;
import java.net.InetAddress;
import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.lang.IllegalArgumentException;

public class FairLossLink extends Link{
    Link caller;

    public FairLossLink(Link caller) {
        super();
        if(caller != null)
            this.caller = caller;
        else {
            throw new IllegalArgumentException("caller is null");
        }
        
    }


    public boolean send(InetAddress destIp, int port, String uid, String msg) throws IOException{
        byte buf[] = msg.getBytes();
        DatagramPacket DpSend = new DatagramPacket(buf, buf.length, destIp, port);
        DatagramSocket ds = new DatagramSocket();
        ds.send(DpSend);
        ds.close();
  
        return true;
    }
    
    public boolean deliver(String msg) {
        caller.deliver(msg);
        return true;
    }
}
