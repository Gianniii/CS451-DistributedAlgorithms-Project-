package cs451.Links;

import java.io.IOException;
import java.net.InetAddress;

public class Sender extends Thread {

    Link perfectLink;
    String msg_uid;
    String msg;
    InetAddress destIp;
    int destPort;

    public Sender(InetAddress destIp, int destPort, PerfectLink perfectLink, String msg_uid, String message) {
        this.perfectLink = perfectLink;
        this.msg_uid = msg_uid;
        this.msg = message;
        this.destIp = destIp;
        this.destPort = destPort;
    }

    @Override
    public void run() {
        try {
            perfectLink.send(destIp,destPort, msg_uid, msg);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}