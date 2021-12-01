package cs451;

import java.util.Collections;
import java.util.List;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Scanner;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class Parser {
    private static final String SPACES_REGEX = "\\s+";
    private String[] args;
    private long pid;
    private IdParser idParser;
    private HostsParser hostsParser;
    private OutputParser outputParser;
    private ConfigParser configParser;

    public Parser(String[] args) {
        this.args = args;
    }

    public void parse() {
        pid = ProcessHandle.current().pid();

        idParser = new IdParser();
        hostsParser = new HostsParser();
        outputParser = new OutputParser();
        configParser = new ConfigParser();

        int argsNum = args.length;
        if (argsNum != Constants.ARG_LIMIT_CONFIG) {
            help();
        }

        if (!idParser.populate(args[Constants.ID_KEY], args[Constants.ID_VALUE])) {
            help();
        }

        if (!hostsParser.populate(args[Constants.HOSTS_KEY], args[Constants.HOSTS_VALUE])) {
            help();
        }

        if (!hostsParser.inRange(idParser.getId())) {
            help();
        }

        if (!outputParser.populate(args[Constants.OUTPUT_KEY], args[Constants.OUTPUT_VALUE])) {
            help();
        }

        if (!configParser.populate(args[Constants.CONFIG_VALUE])) {
            help();
        }
    }

    private void help() {
        System.err.println("Usage: ./run.sh --id ID --hosts HOSTS --output OUTPUT CONFIG");
        System.exit(1);
    }

    public Host myHost() {
        return getHost(myId());
    }
    public int myId() {
        return idParser.getId();
    }

    public List<Host> hosts() {
        return hostsParser.getHosts();
    }

    public String output() {
        return outputParser.getPath();
    }

    public String config() {
        return configParser.getPath();
    }

    /**
     * Parses config file to get hosts affecting me("this")
     * @return a Set where each entry is a Host id of a host I am ("this host/process is") affected by.
     * (i.e a string of the form "pi pk ect.. where pi,pk are processes "this" process is affected by. There could be 0 or many processes this process is affected by)
     */
    //TODO figure out best format to return the processes affecting me.
    public Set<String> getProcessesAffectingMe(){
        String[] splits = {};
        Set<String> affectingHosts = Collections.newSetFromMap(new ConcurrentHashMap<String, Boolean>());
        try(BufferedReader br = new BufferedReader(new FileReader(config()))) {
            int lineNum = 0;
            //for(init something, boolean, increment something)
            for(String line; (line = br.readLine()) != null; lineNum++) {
                if (line.isBlank()) {
                    continue;
                }
                if (lineNum == 0) { //skip first line since it contains m
                    continue;
                }
                splits = line.split(SPACES_REGEX);
                
                if(Integer.valueOf(splits[0]) == myId()){
                    //skip first entry(as it just my contains my own ID)
                    for(int i = 1; i < splits.length; i++){
                        affectingHosts.add(splits[i]);
                    }
                    return affectingHosts;
                }
                lineNum++;
            }
        } catch (IOException e) {
            System.err.println("Incorrect syntax in config file");
        }


        return affectingHosts;
    }


    public int getMessageNumber() {
        File configFile = new File(config());
        int messageNumber = 0;
        try {
            Scanner reader = new Scanner(configFile);
            String data = reader.nextLine().split(" ")[0];
            messageNumber = Integer.parseInt(data);
            reader.close();
        }
        catch (FileNotFoundException e){
            e.printStackTrace();
        }
        return messageNumber;
    }
  
    public Host getHost(int hostId) {
        Host host = null;
        for(Host h: hosts()){
            if(h.getId() == hostId){
                host = h;
            }
        }
        return host;
    }
}
