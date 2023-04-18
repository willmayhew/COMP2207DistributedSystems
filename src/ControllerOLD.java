import java.io.*;
import java.net.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class ControllerOLD {

    private static List<Integer> dstoreList = new ArrayList<>();
    private static Index index = new Index();
    private static int cport;
    private static int r;
    private static int timeout;
    private static int rebalance;
    private static CountDownLatch storeLatch;
    private static CountDownLatch removeLatch;
    private static String currentFile;
    private static PrintWriter storeWriter;
    private static PrintWriter removeWriter;
    private static HashMap<Integer, ArrayList<String>> dstoreHashFiles;

    public static void main(String[] args){

//        cport = Integer.parseInt(args[0]);
//        r = Integer.parseInt(args[1]);
//        timeout = Integer.parseInt(args[2]);
//        rebalance = Integer.parseInt(args[3]);

        cport = 12345;
        r = 1;
        timeout = 5000;
        rebalance = 15000;

        //<Port, List of files in Dstore>
        dstoreHashFiles = new HashMap<>();

        startRebalance(rebalance);

        try{
            ServerSocket ss = new ServerSocket(cport);
            System.out.println("Controller listening on port " + cport);

           storeLatch = null;
           removeLatch = null;
           currentFile = "";

            for(;;){
                try{
                    Socket client = ss.accept();
                    System.out.println("Controller Connection Made");

                    new Thread(() -> {
                        int curPort = 0;
                        int portIndex = 0;
                        try{
                            BufferedReader in = new BufferedReader(new InputStreamReader(client.getInputStream()));
                            PrintWriter out = new PrintWriter(client.getOutputStream(), true);
                            String line;
                            boolean isDstore = false;

                            while((line = in.readLine()) != null) {
                                String[] message = line.split(" ");
                                System.out.println("Received Command: " + line);

                                switch (message[0]){
                                    case Protocol.JOIN_TOKEN:
                                    //dstore join
                                    curPort = Integer.parseInt(message[1]);
                                    dstoreList.add(Integer.parseInt(message[1]));
                                    isDstore = true;
                                    rebalance();
                                    break;
                                    case Protocol.LIST_TOKEN:
                                        if(isDstore){
//                                            dstore list
                                            ArrayList<String> tempList = new ArrayList<>();
                                            for(int i = 1; i<message.length; i++){
                                                tempList.add(message[i]);
                                            }
                                            storeDstoreFiles(tempList, curPort);
                                        } else{
                                            //client list
                                            if(enoughDstores()){
                                                listFiles(out);
                                            } else{
                                                Errors.errorNotEnoughDstores(out);
                                            }
                                        }
                                        break;
                                    case Protocol.STORE_TOKEN:
                                        if(!isDstore){
                                            //client store
                                            storeLatch = new CountDownLatch(r);
                                            if(enoughDstores()){
                                                if(!index.fileExists(message[1]) || !index.isFileRemoved(message[1])){
                                                    currentFile = message[1];
                                                    store(out, message[1], Integer.parseInt(message[2]));
                                                    storeWriter = out;
                                                    client.setSoTimeout(timeout);
                                                } else{
                                                    Errors.errorFileAlreadyExists(out);
                                                }
                                            } else{
                                                Errors.errorNotEnoughDstores(out);
                                            }
                                        }
                                        break;
                                        case Protocol.STORE_ACK_TOKEN:
                                        //dstore store ack
                                        synchronized (storeLatch){
                                            storeLatch.countDown();
                                            if(storeLatch.getCount() == 0){
                                                System.out.println("All STORE_ACKs received");
                                                storeComplete(storeWriter, message[1]);
                                            }
                                        }
                                        break;
                                    case Protocol.LOAD_TOKEN:
                                        //client load
                                        if(enoughDstores()){
                                            loadFile(out, getPort(portIndex), message[1]);
                                        } else{
                                            Errors.errorNotEnoughDstores(out);
                                        }
                                        break;
                                    case Protocol.REMOVE_TOKEN:
                                        //client remove
                                        removeLatch = new CountDownLatch(dstoreList.size());
                                        portIndex = 0;
                                        if(enoughDstores()){
                                            client.setSoTimeout(timeout);
                                            removeFile(out, message[1], getPort((int) (removeLatch.getCount()-1)));
                                            removeWriter = out;
                                            portIndex++;
                                        } else{
                                            Errors.errorNotEnoughDstores(out);
                                        }
                                        break;
                                    case Protocol.REMOVE_ACK_TOKEN:
                                        //dstore remove ack
                                        synchronized (removeLatch){
                                            removeLatch.countDown();
                                            if(removeLatch.getCount() == 0){
                                                System.out.println("All REMOVE_ACKs received");
                                                removeComplete(removeWriter, message[1]);
                                            } else {
                                                System.out.println(removeLatch.getCount());
                                                client.setSoTimeout(timeout);
                                                index.resetState(message[1]);
                                                removeFile(out, message[1], getPort((int) (removeLatch.getCount()-1)));
                                            }
                                        }
                                        break;
                                    case Protocol.RELOAD_TOKEN:
                                        //client reload
                                        portIndex++;
                                        if(dstoreList.size() > portIndex){
                                            loadFile(out, getPort(portIndex), message[1]);
                                        } else{
                                            portIndex = 0;
                                            Errors.errorLoad(out);
                                        }
                                        break;
                                        case Protocol.ACK_TOKEN:
                                        //dstore ack
                                        break;
                                    case Protocol.REBALANCE_STORE_TOKEN:
                                        //dstore rebalance store
                                        break;
                                    case Protocol.REBALANCE_COMPLETE_TOKEN:
                                        //dstore rebalance complete
                                        break;
                                    default:
                                        System.out.println("ERROR: Command not recognised -- " + line);
                                }
                            }
                            //If a store operation is in progress
                            if(storeLatch != null){
                                try{
                                    storeLatch.await();
                                } catch (InterruptedException e){
                                    System.out.println("Error " + e);
                                }
                                if (storeLatch.getCount() > 0) {
                                    // Handle incomplete store operation
                                    index.removeFromIndex(currentFile);
                                }
                            }
                            //If a remove operation is in progress
                            if(removeLatch != null){
                                try{
                                    removeLatch.await();
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
        String message = Protocol.LIST_TOKEN + " ";
        ArrayList<String> fileNames = index.getFileNames();
        for(String fileName : fileNames){
            if(!index.isFileBusy(fileName)){
                message = message + fileName + " ";
            }
        }

        out.println(message);
        System.out.println("Command sent: " + message);
    }

    /**
     * Index updated and STORE_TO message to Client
     * @param out PrintWriter to communicate with client
     * @param filename file to store
     * @param filesize size of file to store
     */
    private static void store(PrintWriter out, String filename, int filesize){
        if(index.isFileBusy(filename)){
            Errors.errorFileAlreadyExists(out);
        } else{
            index.addFile(filename,filesize);

            String message = Protocol.STORE_TO_TOKEN + " ";
            for(int i=0; i<dstoreList.size(); i++){
                message = message + dstoreList.get(i) + " ";
            }
            out.println(message);
            System.out.println("Command sent: " + message);
        }
    }

    /**
     * Store complete message sent to the client when all acks received
     * @param out Print writer
     * @param fileName File name
     */
    private static void storeComplete(PrintWriter out, String fileName){
        try{
            index.addFileComplete(fileName);
            String message = Protocol.STORE_COMPLETE_TOKEN;
            out.println(message);
            System.out.println("Command sent: " + Protocol.STORE_COMPLETE_TOKEN);
        } catch (Exception e){
            System.out.println(e);
        }

    }

    /**
     * Loads a file for the client from a given Dstore (port)
     * @param out Print writer
     * @param port Dstore port
     * @param fileName File name
     */
    private static void loadFile(PrintWriter out, int port, String fileName){
        int fileSize;
        String message;

        if(!index.fileExists(fileName) || index.isFileBusy(fileName)){
            Errors.errorFileDoesNotExist(out);
        }

        fileSize = index.getFileSize(fileName);
        message = Protocol.LOAD_FROM_TOKEN + " " + port + " " + fileSize;
        out.println(message);
        System.out.println("Command sent: " + message);
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
        if(index.fileExists(fileName) && !index.isFileBusy(fileName)){
            index.removeFile(fileName);
            try{
                Socket dstoreSocket = new Socket(InetAddress.getLoopbackAddress(),port);
                PrintWriter dstoreWriter = new PrintWriter(dstoreSocket.getOutputStream(), true);
                String message = Protocol.REMOVE_TOKEN + " " + fileName;
                dstoreWriter.println(message);
                System.out.println("Command sent: " + message);
            } catch(Exception e){System.out.println("Error " + e);}
        } else{
            Errors.errorFileDoesNotExist(out);
        }
    }

    /**
     * Remove complete message sent to the client when all acks received
     * @param out Print writer
     * @param fileName File name
     */
    private static void removeComplete(PrintWriter out, String fileName){
        index.removeFileComplete(fileName);
        out.println(Protocol.REMOVE_COMPLETE_TOKEN);
        System.out.println("Command sent: " + Protocol.REMOVE_COMPLETE_TOKEN);
    }

    /**
     * Rebalance loop start
     * @param rebalance Time period
     */
    private static void startRebalance(Integer rebalance){
        ScheduledExecutorService executor = Executors.newScheduledThreadPool(1);
        executor.scheduleAtFixedRate(() -> rebalance(), rebalance, rebalance, TimeUnit.MILLISECONDS);
    }

    /**
     * Rebalance method
     */
    private static void rebalance(){

        if(!enoughDstores()){
            System.out.println(Protocol.ERROR_NOT_ENOUGH_DSTORES_TOKEN);
            return;
        }

        for(Integer port : dstoreList){
            getDstoreFiles(port);
        }

        for(Integer port : dstoreList){
            System.out.println(port + " " + dstoreHashFiles.get(port));
        }

        fileAllocation();

    }

    /**
     * Gets the files stored in a dstore
     * @param port Dstore port
     * @return List of files
     */
    private static void getDstoreFiles(Integer port){
        //GET FILES FROM DSTORE PORT
        try{
            Socket dstoreSocket = new Socket(InetAddress.getLoopbackAddress(), port);
            PrintWriter dstorePW = new PrintWriter(dstoreSocket.getOutputStream(), true);
            dstorePW.println(Protocol.LIST_TOKEN);
            System.out.println("Command sent: " + Protocol.LIST_TOKEN);
        } catch (Exception e){System.out.println("Error " + e);}
    }

    private static void storeDstoreFiles(ArrayList fileList,Integer port){
        dstoreHashFiles.put(port,fileList);
    }

    private static void fileAllocation(){

    }

}
