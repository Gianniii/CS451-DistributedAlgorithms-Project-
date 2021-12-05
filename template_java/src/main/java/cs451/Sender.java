package cs451;

import java.io.IOException;
import java.net.InetAddress;
import java.util.Random;
import java.util.concurrent.ExecutorService;

import cs451.Broadcasts.*;
import cs451.Links.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Sender extends Thread {

    Link perfectLink;
    Parser parser;
    Broadcast broadcastProtocol;
    boolean terminated = false;
    ExecutorService executorService;
    public Sender(Broadcast FIFO, Parser parser) {
        this.broadcastProtocol = FIFO;
        this.parser = parser;
        //executorService = Executors.newFixedThreadPool(1);
    }

    @Override
    public void run() {
        //Random rand = new Random();
        //avoid too many broadcast at same time
        for (int i = 1; i < parser.getMessageNumber()+1; i++) {
            if(!terminated) {
                String msg_uid = Helper.createUniqueMsgUid(Integer.toString(parser.myId()), Integer.toString(i));
                try {
                    broadcastProtocol.broadcast(msg_uid, String.valueOf(i));
                    //Thread.sleep(rand.nextInt(50)); for testing purposes
                } catch (Exception e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                } 

                /*System.out.println("sender finished status: " + broadcastProtocol.finished());
                while(!broadcastProtocol.finished()){ 
                    try {
                        int sleepTime = 10;
                        Thread.sleep(sleepTime);
                    } catch (InterruptedException e) {}
                }*/
            }
        }
    }

    public boolean close() {
        //executorService.shutdown();
        terminated = true;
        return true;
    }

}