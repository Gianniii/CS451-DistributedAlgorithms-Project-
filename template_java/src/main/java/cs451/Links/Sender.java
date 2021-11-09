package cs451.Links;

import java.io.IOException;
import java.net.InetAddress;
import cs451.Parser;
import cs451.Broadcasts.Broadcast;
import cs451.Broadcasts.FIFOBroadcast;


public class Sender extends Thread {

    Link perfectLink;
    Parser parser;
    Broadcast broadcastProtocol;

    public Sender(Broadcast FIFO, Parser parser) {
        this.broadcastProtocol = FIFO;
        this.parser = parser;
    }

    @Override
    public void run() {
        //TODO THE LOGIC IS WAY DIFF NOW CUZ WE HAD 1 GUY RECEIVING AND ALL OTHERS SENDING SO COULD JUST SEND AND MOVE ON...
        //NOW WE CAN ALSO RECEIVE... DO I NEED TO CHANGE THE LOGIC HERE?? MIGHT HAVE TO CHANGE WHAT I DO UPON A RECEIVE NOT SURE..

        //will here will change perfectLink for uniformRelieableBroacast and uniformRealiableBroadcast will
        for (int i = 0; i < parser.getMessageNumber(); i++) {
            try {
                //TODO: could create message_uid within uniform reliable broadcast
                String msg_uid = Helper.createUniqueMsgUid(Integer.toString(parser.myId()), Integer.toString(i));
                broadcastProtocol.broadcast(msg_uid, String.valueOf(i)); 
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}