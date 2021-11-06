package cs451.Links;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import cs451.Links.StubbornLinkWithAck;

public class Receiver extends Thread {

    private final DatagramSocket socket;
    private final StubbornLinkWithAck stubbornLinkWithAck;
    private boolean running = true;
    
    public Receiver(DatagramSocket socket, StubbornLinkWithAck stubbornLinkWithAck) {
        this.socket = socket;
        this.stubbornLinkWithAck = stubbornLinkWithAck;
    }

    @Override
    public void run(){
        while (running){
            try{
                byte[] buf = new byte[2048];
                DatagramPacket packet = new DatagramPacket(buf, buf.length);
                socket.receive(packet);
                String received = new String(packet.getData(), 0, packet.getLength());
                stubbornLinkWithAck.deliver(received);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        socket.close();
    }

    public void close() {
        running = false;
    }
}