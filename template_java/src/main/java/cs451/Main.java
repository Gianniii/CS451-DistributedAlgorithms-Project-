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
import cs451.Links.*;


public class Main {

    private static void handleSignal(PerfectLink pLink, String filePath, Receiver receiver, ExecutorService executor) {
        //immediately stop network packet processing
        System.out.println("Immediately stopping network packet processing.");
        receiver.close();
        executor.shutdownNow();
        //write/flush output file if necessary
        System.out.println("Writing output.");
        writeLogToFile(filePath, pLink.getLogs());
    }

    private static void initSignalHandlers(PerfectLink pLink, String filePath, Receiver receiver, ExecutorService executor) {
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                handleSignal(pLink, filePath, receiver, executor);
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

        int dstId = parser.getDestination();
        Host dstHost = parser.getHost(dstId);
        if(dstHost == null) {
            throw new IllegalArgumentException("Null host");
        }

        DatagramSocket socket= new DatagramSocket(parser.myHost().getPort());
        Receiver receiver = new Receiver(socket, stubbornLink);
        receiver.start();

        System.out.println("Broadcasting and delivering messages...\n");
        
        ExecutorService executor = Executors.newFixedThreadPool(10);
        if (parser.myId() != dstId) {
            for (int i = 0; i < parser.getMessageNumber(); i++) {
                String msg_uid = Helper.createUniqueMsgUid(Integer.toString(parser.myId()), Integer.toString(i));
                executor.execute(new Sender(InetAddress.getByName(dstHost.getIp()), dstHost.getPort(),
                perfectLink, msg_uid, String.valueOf(i)));
            }
        }


        initSignalHandlers(perfectLink, parser.output(), receiver, executor);

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
