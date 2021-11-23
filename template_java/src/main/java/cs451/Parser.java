package cs451;

import java.util.List;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Scanner;

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

    /**
     * @return line in config file containing the processes this process is affected by 
     * (i.e a string of the form "myId pi pk" ect.. where pi,pk are processes i am affected by there could be 0 or many processes this process is affected by)
     */
    //TODO figure out best format to return the processes affecting me.
    public String[] getProcessesAffectingMe() throws IOException {
        String[] splits = {};
        try(BufferedReader br = new BufferedReader(new FileReader(config()))) {
            int lineNum = 1;
            //for(init something, boolean, increment something)
            for(String line; (line = br.readLine()) != null; lineNum++) {
                if (lineNum == 0) { //skip first line since it contains m
                    continue;
                }
                
                splits = line.split(SPACES_REGEX);
            
                if(Integer.valueOf(splits[0]) == myId()){
                    return splits;
                }
            }
        } catch (IOException e) {
            System.err.println("Problem with the hosts file!");
        }


        return splits;
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
