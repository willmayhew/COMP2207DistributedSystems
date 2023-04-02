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
                                                System.out.println("Command sent: ERROR_NOT_ENOUGH_DSTORES");
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
                                                    System.out.println("Command sent: ERROR_FILE_ALREADY_EXISTS");
                                                }
                                            } else{
                                                out.println("ERROR_NOT_ENOUGH_DSTORES");
                                                System.out.println("Command sent: ERROR_NOT_ENOUGH_DSTORES");
                                            }
                                        }
                                        break;
                                    case "LOAD":
                                        //client load
                                        int fileSize = index.getFileSize(message[1]);
                                        loadFile(out, fileSize);
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
                                        System.out.println("Command sent: STORE_COMPLETE");
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
        String message = "LIST ";
        ArrayList<String> fileNames = index.getFileNames();
        for(String fileName : fileNames){
            message = message + fileName + " ";
        }

        out.println(message);
        System.out.println("Command sent: LIST");
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

        String message = "STORE_TO ";
        for(int i=0; i<dstoreList.size(); i++){
            message = message + dstoreList.get(i) + " ";
        }
        out.println(message);
        System.out.println("Command sent: STORE_TO");
    }

    private static void loadFile(PrintWriter out, int fileSize){
        String message = "LOAD_FROM ";

        if(dstoreExists()){
            message = message + dstoreList.get(0) + " " + fileSize;
            System.out.println("Command sent: LOAD_FROM");
        } else {
            message = "ERROR_NOT_ENOUGH_DSTORES";
            System.out.println("Command sent: ERROR_NOT_ENOUGH_DSTORES");
        }

        out.println(message);
    }

    private static boolean dstoreExists(){
        return dstoreList.size() > 0;
    }

    private static void rebalance(){

    }

}
