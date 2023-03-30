import java.io.*;
import java.net.*;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

public class Controller {

    private static List<Integer> dstoreList = new ArrayList<>();

    private static int cport;
    private static int r;
    private static int timeout;
    private static int rebalance;

    public static void main(String[] args){

        cport = Integer.parseInt(args[0]);
        r = Integer.parseInt(args[1]);
        timeout = Integer.parseInt(args[2]);
        rebalance = Integer.parseInt(args[3]);

        try{
            ServerSocket ss = new ServerSocket(cport);
            System.out.println("Listening on port " + cport);
            for(;;){
                try{
                    Socket client = ss.accept();
                    System.out.println("Connected");
                    BufferedReader in = new BufferedReader(new InputStreamReader(client.getInputStream()));
                    PrintWriter out = new PrintWriter(client.getOutputStream(), true);
                    String line;
                    boolean dstore = false;
                    while((line = in.readLine()) != null) {
                        String[] message = line.split(" ");
                        System.out.println("Received Command: " + message[0]);

                        switch (message[0]){
                            case "LIST":
                                if(dstore){
                                    //dstore list
                                } else{
                                    //client list
                                }
                                break;
                            case "STORE":
                                if(dstore){
                                    //dstore store
                                } else{
                                    //client store
                                    store(out, message[1], Integer.parseInt(message[2]));
                                }
                                break;
                            case "LOAD":
                                //client load
                                break;
                            case "LOAD_DATA":
                                //client load data
                                break;
                            case "RELOAD":
                                //client reload
                                break;
                            case "REMOVE":
                                //client remove
                                break;
                            case "ACK":
                                //dstore ack
                                break;
                            case "STORE_ACK":
                                //dstore store ack
                                break;
                            case "REMOVE_ACK":
                                //dstore remove ack
                                break;
                            case "JOIN":
                                //dstore join
                                dstoreList.add(Integer.parseInt(message[1]));
                                dstore = true;
                                rebalance();
                                break;
                            case "REBALANCE_STORE":
                                //dstore rebalance store
                                break;
                            case "REBALANCE_COMPLETE":
                                //dstore rebalance complete
                                break;
                        }
                    }
                } catch (Exception e){System.out.println("Error " + e);}
            }
        }catch (Exception e){System.out.println("Error " + e);}

    }

    /**
     * STORE_TO message to Client
     * @param out PrintWriter to communicate with client
     * @param filename file to store
     * @param filesize size of file to store
     */
    private static void store(PrintWriter out, String filename, int filesize){

    }

    private static void rebalance(){

    }

}
