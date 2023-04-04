import jdk.swing.interop.SwingInterOpUtils;

import java.io.*;
import java.net.*;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

public class Controller {

    private static List<Integer> dstoreList = new ArrayList<>();
    private static Index index = new Index();
    private static int cport;
    private static int r;
    private static int timeout;
    private static int rebalance;
    private static CountDownLatch storeLatch;
    private static CountDownLatch removeLatch;

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

           storeLatch = null;
           removeLatch = null;

            for(;;){
                try{
                    Socket client = ss.accept();
                    System.out.println("Controller Connected");

                    new Thread(() -> {
                        int portIndex = 0;
                        int totalStores = 0;
                        try{
                            BufferedReader in = new BufferedReader(new InputStreamReader(client.getInputStream()));
                            PrintWriter out = new PrintWriter(client.getOutputStream(), true);
                            String line;
                            boolean dstore = false;
                            while((line = in.readLine()) != null) {
                                String[] message = line.split(" ");
                                System.out.println("Received Command: " + line);

                                switch (message[0]){
                                    case "LIST":
                                        if(dstore){
                                            //dstore list
                                        } else{
                                            //client list
                                            if(enoughDstores()){
                                                client.setSoTimeout(timeout);
                                                listFiles(out);
                                            } else{
                                                Errors.errorNotEnoughDstores(out);
                                            }
                                        }
                                        break;
                                    case "STORE":
                                        if(!dstore){
                                            //client store
                                            storeLatch = new CountDownLatch(r);
                                            if(enoughDstores()){
                                                if(!index.fileExists(message[1])){
                                                    store(out, message[1], Integer.parseInt(message[2]));
                                                    client.setSoTimeout(timeout);
                                                } else{
                                                    Errors.errorFileAlreadyExists(out);
                                                }
                                            } else{
                                                Errors.errorNotEnoughDstores(out);
                                            }
                                        }
                                        break;
                                    case "LOAD":
                                        //client load
                                        if(enoughDstores()){
                                            loadFile(out, getPort(portIndex), message[1]);
                                        } else{
                                            Errors.errorNotEnoughDstores(out);
                                        }
                                        break;
                                    case "RELOAD":
                                        //client reload
                                        portIndex++;
                                        if(dstoreList.size() > portIndex){
                                            loadFile(out, getPort(portIndex), message[1]);
                                        } else{
                                            portIndex = 0;
                                            Errors.errorLoad(out);
                                        }
                                        break;
                                    case "REMOVE":
                                        //client remove
                                        removeLatch = new CountDownLatch(dstoreList.size());
                                        portIndex = 0;
                                        if(enoughDstores()){
                                            client.setSoTimeout(timeout);
                                            removeFile(out, message[1], getPort(dstoreList.size()-1));
                                            portIndex++;
                                        } else{
                                            Errors.errorNotEnoughDstores(out);
                                        }



                                        break;
                                    case "ACK":
                                        //dstore ack
                                        break;
                                    case "STORE_ACK":
                                        //dstore store ack
//                                        totalStores++;
//
//                                        if(totalStores == r){
//                                            totalStores = 0;
//                                            storeComplete(out, message[1]);
//                                        }

                                        synchronized (storeLatch){
                                            storeLatch.countDown();
                                            if(storeLatch.getCount() == 0){
                                                System.out.println("All STORE_ACKs received");
                                                storeComplete(out, message[1]);
                                            }
                                        }

                                        break;
                                    case "REMOVE_ACK":
                                        //dstore remove ack
                                        if(dstoreList.size() > portIndex){
                                            client.setSoTimeout(timeout);
                                            removeFile(out,message[1], getPort(portIndex));
                                            portIndex++;
                                        } else{
                                            removeComplete(out, message[1]);
                                        }

                                        synchronized (removeLatch){
                                            removeLatch.countDown();
                                            if(removeLatch.getCount() == 0){
                                                System.out.println("All REMOVE_ACKs received");
                                                removeComplete(out, message[1]);
                                            } else {
                                                client.setSoTimeout(timeout);
                                                removeFile(out, message[1], getPort((int) (removeLatch.getCount()-1)));
                                            }
                                        }

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
                            if(storeLatch != null){
                                try{
                                    storeLatch.await();
                                } catch (InterruptedException e){
                                    System.out.println("Error " + e);
                                }
                            }
                            client.close();
                        }catch (SocketTimeoutException e){System.out.println("Socket timeout");}
                        catch (Exception e){System.out.println("Error " + e);}
                    }).start();
                }catch (Exception e){System.out.println("Error " + e);}
            }
        }catch (Exception e){System.out.println("Error " + e);}

    }

    /**
     * Checks if there is enough Dstores relative to r
     * @return More than or equal to r Dstores
     */
    private static boolean enoughDstores(){
        return dstoreList.size() >= r;
    }

    /**
     * Sends a List message with all the different files stored
     * @param out Print writer
     */
    private static void listFiles(PrintWriter out){
        String message = "LIST ";
        ArrayList<String> fileNames = index.getFileNames();
        for(String fileName : fileNames){
            if(!index.fileBusy(fileName)){
                message = message + fileName + " ";
            }
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
        if(index.fileBusy(filename)){
            Errors.errorFileAlreadyExists(out);
        } else{
            index.addFile(filename,filesize);

            String message = "STORE_TO ";
            for(int i=0; i<dstoreList.size(); i++){
                message = message + dstoreList.get(i) + " ";
            }
            out.println(message);
            System.out.println("Command sent: STORE_TO");
        }
    }

    private static void storeComplete(PrintWriter out, String fileName){
        index.addFileComplete(fileName);
        out.println("STORE_COMPLETE");
        System.out.println("Command sent: STORE_COMPLETE");
    }

    /**
     * Loads a file for the client from a given Dstore (port)
     * @param out Print writer
     * @param port Dstore port
     * @param fileName File name
     */
    private static void loadFile(PrintWriter out, int port, String fileName){
        int fileSize;

        if(!index.fileExists(fileName) || index.fileBusy(fileName)){
            Errors.errorFileDoesNotExist(out);
        }

        fileSize = index.getFileSize(fileName);
        out.println("LOAD_FROM " + port + " " + fileSize);
        System.out.println("Command sent: LOAD_FROM");
    }

    private static int getPort(int index){
        return dstoreList.get(index);
    }

    /**
     * Removes a file with a given name from a given Dstore
     * @param out Print writer
     * @param fileName File name
     * @param port Dstore port
     */
    private static void removeFile(PrintWriter out, String fileName, int port){
        if(index.fileExists(fileName) && !index.fileBusy(fileName)){
            index.removeFile(fileName);
            try{
                Socket dstoreSocket = new Socket(InetAddress.getLoopbackAddress(),port);
                PrintWriter dstoreWriter = new PrintWriter(dstoreSocket.getOutputStream(), true);
                dstoreWriter.println("REMOVE " + fileName);
                System.out.println("Command sent: REMOVE");
            } catch(Exception e){System.out.println("Error " + e);}
        } else{
            Errors.errorFileDoesNotExist(out);
        }
    }

    private static void removeComplete(PrintWriter out, String fileName){
        index.removeFileComplete(fileName);
        out.println("REMOVE_COMPLETE");
        System.out.println("Command sent: REMOVE_COMPLETE");
    }

    private static void rebalance(){

    }

}
