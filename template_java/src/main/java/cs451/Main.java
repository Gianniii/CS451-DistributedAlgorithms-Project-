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
import cs451.Links.*;


public class Main {

    private static void handleSignal(PerfectLink pLink, String filePath) {
        //TODO does this method handle SIGINT SIGTERM?????
        //immediately stop network packet processing
        System.out.println("Immediately stopping network packet processing.");
        
        //write/flush output file if necessary
        System.out.println("Writing output.");
        writeLogToFile(filePath, pLink.getLogs());
    }

    private static void initSignalHandlers(PerfectLink pLink, String filePath) {
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                handleSignal(pLink, filePath);
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
       
        PerfectLink perfectLink = new PerfectLink(parser);
        StubbornLinkWithAck stubbornLink = perfectLink.getStubbornLink();
        
      
        initSignalHandlers(perfectLink, parser.output());

        int dstId = parser.getDestination();
        Host dstHost = parser.getHost(dstId);
        if(dstHost == null) {
            throw new IllegalArgumentException("Null host");
        }

        Receiver receiver = new Receiver(parser.myHost().getPort(), stubbornLink);
        receiver.start();

        System.out.println("Broadcasting and delivering messages...\n");
        //TODO CHANGE THIS ?? check how others do it
        //TODO SHOULD I USE CONCURENT DATASTRUCTURES FOR EVERYTHING I.E EX: QUEUE IN STUBBONLINK
        //TODO run the stress.py ect... LOOK at slides for tips on testing ect...
        if (parser.myId() != dstId) {
            ExecutorService executor = Executors.newFixedThreadPool(5); //why 5 ??? 
            for (int i = 0; i < parser.getMessageNumber(); i++) {
                String msg_uid = Helper.createUniqueMsgUid(Integer.toString(parser.myId()), 
                    Integer.toString(i));
                Sender obj = new Sender(InetAddress.getByName(dstHost.getIp()), dstHost.getPort(),
                    perfectLink, msg_uid, String.valueOf(i) 
                    );
                
                executor.execute(obj);
            }
        }

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
            stringBuilder.append(l).append("\n");
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
