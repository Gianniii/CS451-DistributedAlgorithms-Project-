package cs451.Links;

import java.io.IOException;
import java.net.InetAddress;
import cs451.Parser;

public class Sender extends Thread {

    Link perfectLink;
    String msg_uid;
    String msg;
    InetAddress destIp;
    int destPort;
    Parser parser;

    public Sender(InetAddress destIp, int destPort, PerfectLink perfectLink, Parser parser) {
        this.perfectLink = perfectLink;
        this.destIp = destIp;
        this.destPort = destPort;
        this.parser = parser;
    }

    @Override
    public void run() {
        if (parser.myId() != parser.getDestination()) {
            for (int i = 0; i < parser.getMessageNumber(); i++) {
                try {
                    String msg_uid = Helper.createUniqueMsgUid(Integer.toString(parser.myId()), Integer.toString(i));
                    perfectLink.send(destIp, destPort, msg_uid, String.valueOf(i));
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}