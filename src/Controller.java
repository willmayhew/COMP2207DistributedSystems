import java.io.*;
import java.net.*;
import java.util.ArrayList;
import java.util.List;

public class Controller {

    private static List<Integer> dstoreList = new ArrayList<>();
    private static Index index = new Index();
    private static int cport;
    private static int r;
    private static int timeout;
    private static int rebalance;

    public static void main(String[] args){

//        cport = Integer.parseInt(args[0]);
//        r = Integer.parseInt(args[1]);
//        timeout = Integer.parseInt(args[2]);
//        rebalance = Integer.parseInt(args[3]);

        cport = 12345;
        r = 1;
        timeout = 5000;
        rebalance = 15000;

        try{
            ServerSocket ss = new ServerSocket(cport);
            System.out.println("Controller listening on port " + cport);
            for(;;){
                try{
                    Socket client = ss.accept();
                    System.out.println("Controller Connected");

                    new Thread(() -> {
                        try{
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
                                            if(enoughDstores()){
                                                listFiles(out);
                                            } else{
                                                out.println("ERROR_NOT_ENOUGH_DSTORES");
                                                System.out.println("Error - Not enough Dstores");
                                            }

                                        }
                                        break;
                                    case "STORE":
                                        if(dstore){
                                            //dstore store
                                        } else{
                                            //client store
                                            if(enoughDstores()){
                                                if(!index.fileExists(message[1])){
                                                    store(out, message[1], Integer.parseInt(message[2]));
                                                } else{
                                                    out.println("ERROR_FILE_ALREADY_EXISTS");
                                                    System.out.println("Error - File already exists");
                                                }
                                            } else{
                                                out.println("ERROR_NOT_ENOUGH_DSTORES");
                                                System.out.println("Error - Not enough dstores");
                                            }
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
                                        out.println("STORE_COMPLETE");
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
                            client.close();
                        }catch (Exception e){System.out.println("Error " + e);}
                    }).start();
                } catch (Exception e){System.out.println("Error " + e);}
            }
        }catch (Exception e){System.out.println("Error " + e);}

    }

    private static boolean enoughDstores(){
        return dstoreList.size() >= r;
    }

    private static void listFiles(PrintWriter out){
        String message = "LIST";
        ArrayList<String> fileNames = index.getFileNames();
        for(String fileName : fileNames){
            message = message + fileName + " ";
        }

        out.println(message);
    }

    /**
     * Index updated and STORE_TO message to Client
     * @param out PrintWriter to communicate with client
     * @param filename file to store
     * @param filesize size of file to store
     */
    private static void store(PrintWriter out, String filename, int filesize){
        index.addFile(filename,filesize);
        System.out.println("Index Update: " + filename + " Stored");

        String message = "STORE_TO";
        for(int i=0; i<dstoreList.size(); i++){
            message = message + dstoreList.get(i) + " ";
        }
        out.println(message);
    }

    private static void rebalance(){

    }

}
