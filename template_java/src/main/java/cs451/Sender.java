package cs451;

import java.io.IOException;
import java.net.InetAddress;

import cs451.Broadcasts.*;
import cs451.Links.*;

public class Sender extends Thread {

    Link perfectLink;
    Parser parser;
    Broadcast broadcastProtocol;
    boolean terminated = false;
    public Sender(Broadcast FIFO, Parser parser) {
        this.broadcastProtocol = FIFO;
        this.parser = parser;
    }

    @Override
    public void run() {
        for (int i = 1; i < parser.getMessageNumber()+1; i++) {
            if(!terminated) {
                try {
                    String msg_uid = Helper.createUniqueMsgUid(Integer.toString(parser.myId()), Integer.toString(i));
                    broadcastProtocol.broadcast(msg_uid, String.valueOf(i)); 
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public boolean close() {
        terminated = true;
        return true;
    }

}