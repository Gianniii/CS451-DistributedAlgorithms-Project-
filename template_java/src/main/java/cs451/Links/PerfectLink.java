package cs451.Links;
import java.net.InetAddress;
import java.io.IOException;

public class PerfectLink extends Link{
    StubbornLink stubbornLink;

    public PerfectLink() {
        stubbornLink = new StubbornLink(this);
    }

    public boolean send(InetAddress destIp, int port, String uid, String msg) throws IOException {
        stubbornLink.send(destIp, port, uid, msg);
        return true;
    }
    
    public boolean deliver(String msg){
        //print to log for now, in future will beb, if im sure my thing works can even print to log directly when sending
        return true;
    }
    
}
