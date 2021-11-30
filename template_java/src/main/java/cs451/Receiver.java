package cs451;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import cs451.Links.StubbornLinkWithAck;

public class Receiver extends Thread {

    private final DatagramSocket socket;
    private final StubbornLinkWithAck stubbornLinkWithAck;
    private boolean running = true;
    Parser parser;
    ExecutorService executorService;
    
    public Receiver(DatagramSocket socket, StubbornLinkWithAck stubbornLinkWithAck, Parser parser) {
        this.socket = socket;
        this.stubbornLinkWithAck = stubbornLinkWithAck;
        executorService = Executors.newFixedThreadPool(100);
        this.parser = parser;
    }

    @Override
    public void run(){
        while (running){
            try{
                byte[] buf = new byte[2048];
                DatagramPacket packet = new DatagramPacket(buf, buf.length);
                socket.receive(packet);
                String received = new String(packet.getData(), 0, packet.getLength());
                //System.out.println("received packet: " + received);
                
                if(running) {
                    /*class Task implements Callable<String> {
                        @Override
                        public String call() throws Exception {
                            stubbornLinkWithAck.deliver(received); 
                            return "Ready!";
                        }
                    }
                    ExecutorService executor = Executors.newSingleThreadExecutor();
                    Future<String> future = executor.submit(new Task());
            
                    try {
                        System.out.println("Started..");
                        System.out.println(future.get(10, TimeUnit.SECONDS));
                        System.out.println("Finished!");
                    } catch (TimeoutException e) {
                        System.out.println("TIMEOUT");
                        future.cancel(true);
                    } catch (InterruptedException e) {
                        System.out.println("InterruptedException");
                        future.cancel(true);
                        e.printStackTrace();
                    } catch (ExecutionException e) {
                        future.cancel(true);
                        e.printStackTrace();
                    } finally {
                        executor.shutdownNow();
                    }*/
                    Thread t1 = new Thread(new Runnable() {
                        @Override //Treat received packet in new thread so i can continue listening 
                        public void run() {
                            try {
                                stubbornLinkWithAck.deliver(received);
                            } catch (IOException e) {
                                // TODO Auto-generated catch block
                                e.printStackTrace();
                            }
                        }
                    });
                    t1.start(); 
                }  //executorService.execute(t1); 
                } catch (IOException e) {
                e.printStackTrace();
            }
        }
        socket.close();
    }

    public void close() {
        //executorService.shutdown();
        running = false;
    }
}