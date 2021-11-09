package cs451;

import java.io.IOException;
import java.util.concurrent.Executors;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.net.InetAddress;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.lang.IllegalArgumentException;
import cs451.Links.PerfectLink;
import java.io.File;
import java.io.FileWriter;
import java.net.DatagramSocket;

import cs451.Broadcasts.FIFOBroadcast;
import cs451.Broadcasts.UniformReliableBroadcast;
import cs451.Links.*;


public class Main {

    private static void handleSignal(UniformReliableBroadcast URB, String filePath, Receiver receiver, Sender sender) {
        //immediately stop network packet processing
        System.out.println("Immediately stopping network packet processing.");
        receiver.close();
        receiver.interrupt();
        sender.interrupt();
        //write/flush output file if necessary
        System.out.println("Writing output.");
        writeLogToFile(filePath, URB.getLogs());
    }

    private static void initSignalHandlers(UniformReliableBroadcast URB, String filePath, Receiver receiver, Sender sender) {
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                handleSignal(URB, filePath, receiver, sender);
            }
        });
    }

    public static void main(String[] args) throws InterruptedException, UnknownHostException, SocketException{
        Parser parser = new Parser(args);
        parser.parse();

        // example
        long pid = ProcessHandle.current().pid();
        System.out.println("My PID: " + pid + "\n");
        System.out.println("From a new terminal type `kill -SIGINT " + pid + "` or `kill -SIGTERM " + pid + "` to stop processing packets\n");

        System.out.println("My ID: " + parser.myId() + "\n");
        System.out.println("List of resolved hosts is:");
        System.out.println("==========================");
        for (Host host: parser.hosts()) {
            System.out.println(host.getId());
            System.out.println("Human-readable IP: " + host.getIp());
            System.out.println("Human-readable Port: " + host.getPort());
            System.out.println();
        }
        System.out.println();

        System.out.println("Path to output:");
        System.out.println("===============");
        System.out.println(parser.output() + "\n");

        System.out.println("Path to config:");
        System.out.println("===============");
        System.out.println(parser.config() + "\n");

        System.out.println("Doing some initialization\n");
       
    
        FIFOBroadcast FIFO = new FIFOBroadcast(parser);
        UniformReliableBroadcast URB = new UniformReliableBroadcast(parser, FIFO);
        PerfectLink perfectLink = URB.getBestEffortBroadcast().getPerfectLink();
        StubbornLinkWithAck stubbornLink = perfectLink.getStubbornLink();
        
        //Now probably need do processes = parser.getProcesses() or something like that.. can do this within

        //sender and receiver need to be threads, so that we can send and receive in parallel
        DatagramSocket socket= new DatagramSocket(parser.myHost().getPort());
        Receiver receiver = new Receiver(socket, stubbornLink);
        Sender sender = new Sender(FIFO, parser);
        receiver.start();
        System.out.println("Broadcasting and delivering messages...\n");
        sender.start();
    


        initSignalHandlers(URB, parser.output(), receiver, sender);

        // After a process finishes broadcasting,
        // it waits forever for the delivery of messages.
        while (true) {
            // Sleep for 1 hour
            Thread.sleep(60 * 60 * 1000);
        }
    }


    private static void writeLogToFile(String outPath, ConcurrentLinkedQueue<String> log) {
        StringBuilder stringBuilder = new StringBuilder();
        for(String l: log) {
            stringBuilder.append(l + "\n");
        }

        try {
            File file = new File(outPath);
            FileWriter fileWriter = new FileWriter(file);
            fileWriter.write(stringBuilder.toString());
            fileWriter.flush();
            fileWriter.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
