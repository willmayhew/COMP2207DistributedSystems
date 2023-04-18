import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.util.*;
import java.util.concurrent.*;

public class Controller {

    private static List<Integer> dstoreList = new ArrayList<>();
    private static Hashtable<Integer, ArrayList<String>> dstoreFiles;
    private static Index index = new Index();
    private static int cport;
    private static int r;
    private static int timeout;
    private static int rebalance;
    private static String currentFile;
    private static CountDownLatch storeLatch = null;
    private static CountDownLatch removeLatch = null;
    private static Hashtable<Integer, ArrayList<String>> dstoreRebalanceHash;
    private static Hashtable<String, PrintWriter> storeWriterHash;
    private static Hashtable<String, PrintWriter> removeWriterHash;
    private static boolean rebalancing = false;
    private static boolean storingNow = false;
    private static boolean removingNow = false;

    public static void main(String[] args){

        cport = Integer.parseInt(args[0]);
        r = Integer.parseInt(args[1]);
        timeout = Integer.parseInt(args[2]);
        rebalance = Integer.parseInt(args[3]);

//        cport = 12345;
//        r = 1;
//        timeout = 5000;
//        rebalance = 15000;

        storeWriterHash = new Hashtable<>();
        removeWriterHash = new Hashtable<>();

        startRebalance(rebalance);

        try{
            ServerSocket ss = new ServerSocket(cport);
            System.out.println("Controller listening on port " + cport);

            for(;;){
                Socket client = ss.accept();
                System.out.println("Controller Connection Made");

                new Thread(() -> {
                    int portIndex = 0;
                    try{
                        BufferedReader in = new BufferedReader(new InputStreamReader(client.getInputStream()));
                        PrintWriter out = new PrintWriter(client.getOutputStream(), true);
                        boolean dStore = false;

                        while((!dStore)){

//                            while(rebalancing);

//                            while(storingNow || removingNow);

                            try{
                                String line = in.readLine();
                                if(line != null){

                                    String message[] = line.split(" ");
                                    System.out.println("---------------------------------------------");
                                    System.out.println("Received Command: " + line);

                                    synchronized (in){
                                        switch (message[0]){
                                            case Protocol.JOIN_TOKEN:
                                                dStore = true;
                                                new Thread(() -> dstoreJoined(client, Integer.valueOf(message[1]))).start();
                                                break;
                                            case Protocol.LIST_TOKEN:
                                                if(enoughDstores()){
                                                    listFiles(out);
                                                } else{
                                                    Errors.errorNotEnoughDstores(out);
                                                }
                                                break;
                                            case Protocol.STORE_TOKEN:
                                                if(enoughDstores()){
                                                    storeLatch = new CountDownLatch(r);
                                                    if(storeWriterHash.get(message[1]) == null){
                                                        storeWriterHash.put(message[1], out);
                                                        if(index.canStore(message[1])){
                                                            store(out, message[1], Integer.parseInt(message[2]));
//                                                    storeWriter = out;
                                                            currentFile = message[1];
                                                            client.setSoTimeout(timeout);
                                                        } else{
                                                            Errors.errorFileAlreadyExists(out);
                                                            System.out.println(message[1] + " ----- " + index.getFileState(message[1]));
                                                        }
                                                    } else{
                                                        Errors.errorFileAlreadyExists(out);
                                                        System.out.println(storeWriterHash.get(message[1]));
                                                    }
                                                } else{
                                                    Errors.errorNotEnoughDstores(out);
                                                }
                                                break;
                                            case Protocol.REMOVE_TOKEN:
                                                if(enoughDstores()){
                                                    removeLatch = new CountDownLatch(dstoreList.size());
                                                    if(removeWriterHash.get(message[1]) == null){
                                                        removeWriterHash.put(message[1], out);
                                                        if(index.fileExists(message[1]) && index.canRemove(message[1])){
                                                            index.removeFile(message[1]);
                                                            removeFile(message[1], getPort((int) (removeLatch.getCount()-1)));
                                                            client.setSoTimeout(timeout);
                                                        } else{
                                                            Errors.errorFileDoesNotExist(out);
//                                                            sendCommand(out,Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN);
                                                        }
                                                    } else{
                                                        Errors.errorFileDoesNotExist(out);
                                                    }
                                                } else{
                                                    Errors.errorNotEnoughDstores(out);
                                                }
                                                break;
                                            case Protocol.LOAD_TOKEN:
                                                if(enoughDstores()){
                                                    loadFile(out,getPort(portIndex), message[1]);
                                                } else{
                                                    Errors.errorNotEnoughDstores(out);
                                                }
                                                break;
                                            case Protocol.RELOAD_TOKEN:
                                                portIndex++;
                                                if(dstoreList.size() > portIndex){
                                                    loadFile(out, getPort(portIndex), message[1]);
                                                } else{
                                                    portIndex = 0;
                                                    Errors.errorLoad(out);
                                                }
                                                break;
                                            default:
                                                System.out.println("ERROR: Command not recognised -- " + line);
                                        }
                                    }

                                }
                            } catch (SocketTimeoutException e){System.out.println("Socket timed out waiting for response");}
                        }
                    } catch(Exception e){System.out.println("Error " + e);}

                }).start();

            }
        } catch (Exception e){System.out.println("Error " + e);}

    }

    private static void dstoreJoined(Socket dstore, Integer port){
        if(!dstoreList.contains(port)){
            dstoreList.add(port);
            rebalance();

            try{
                BufferedReader in = new BufferedReader(new InputStreamReader(dstore.getInputStream()));
                PrintWriter out = new PrintWriter(dstore.getOutputStream());
                String line;

                while((line = in.readLine()) != null){

                    String[] message = line.split(" ");
                    System.out.println("---------------------------------------------");
                    System.out.println("Received Command: " + line);
                    switch(message[0]){
                        case Protocol.LIST_TOKEN:
                            rebalanceList(port, line);
                            break;
                        case Protocol.STORE_ACK_TOKEN:
                            synchronized (storeLatch){
                                storeLatch.countDown();
                                if(storeLatch.getCount() == 0){
                                    System.out.println("All STORE_ACKs received");
                                    storeComplete(storeWriterHash.get(message[1]), message[1]);
                                }
                            }
                            break;
                        case Protocol.REMOVE_ACK_TOKEN:
                            synchronized (removeLatch){
                                removeLatch.countDown();
                                if(removeLatch.getCount() == 0){
                                    System.out.println("All REMOVE_ACKs received");
                                    removeComplete(removeWriterHash.get(message[1]), message[1]);
                                } else{
                                    dstore.setSoTimeout(timeout);
                                    removeFile(message[1], getPort((int) (removeLatch.getCount()-1)));
                                }
                            }
                            break;
                        case Protocol.REBALANCE_COMPLETE_TOKEN:
                            rebalancing = false;
                        default:
                            System.out.println("ERROR: Command not recognised -- " + line);
                    }
                }
                if(storeLatch != null){
                    try{
                        storeLatch.await();
                    } catch (InterruptedException e){
                        System.out.println("Error " + e);
                    }
//                    if(storeLatch.getCount() > 0){
//                        index.removeFromIndex(currentFile);
//                    }
                }
                if(removeLatch != null){
                    try{
                        removeLatch.await();
                    } catch (InterruptedException e){
                        System.out.println("Error " + e);
                    }
                }
            } catch (SocketTimeoutException e){
                System.out.println("Socket timed out waiting for response");
                if(storeLatch != null && storeLatch.getCount() > 0){
                    index.removeFromIndex(currentFile);
                }
                if(dstore.isClosed()){
                    dstoreFailed(port);
                }
            } catch (Exception e){
                System.out.println("Error " + e);
            }
        }
    }

    private static synchronized void dstoreFailed(Integer port){

        ArrayList<String> filesToRemove = new ArrayList<>();

        for(String file : dstoreRebalanceHash.get(port)){
            boolean found = false;
            for(int otherPorts : dstoreRebalanceHash.keySet()){
                if(otherPorts != port && dstoreRebalanceHash.get(otherPorts).contains(file)){
                    found = true;
                    break;
                }
            }
            if(!found){
                filesToRemove.add(file);
            }
        }

        for(String fileToRemove : filesToRemove){
            index.removeFromIndex(fileToRemove);
        }

        System.out.println("Files stored in dstore " + port + " removed from index");

        dstoreList.remove(port);

        System.out.println("Dstore " + port + " removed");

    }

    private static synchronized void sendCommand(PrintWriter out, String command) {
        out.println(command);
    }

    /**
     * Checks if there is enough Dstores relative to r
     * @return More than or equal to r Dstores
     */
    private static synchronized boolean enoughDstores(){
        return dstoreList.size() >= r;
    }

    /**
     * Sends a List message with all the different files stored
//     * @param out Print writer
     */
    private static void listFiles(PrintWriter out){
        String message = Protocol.LIST_TOKEN + " ";
        ArrayList<String> fileNames = index.getFileNames();
        for(String fileName : fileNames){
            if(index.isFileStored(fileName)){
                message = message + fileName + " ";
            }
        }

//        out.println(message);
        sendCommand(out,message);
        System.out.println("Command sent: " + message);
    }

    /**
     * Index updated and STORE_TO message to Client
     * @param out PrintWriter to communicate with client
     * @param filename file to store
     * @param filesize size of file to store
     */
    private static synchronized void store(PrintWriter out, String filename, int filesize){
        if(index.isFileBusy(filename)){
            Errors.errorFileAlreadyExists(out);
        } else{
            index.addFile(filename,filesize);
            storingNow = true;

            String message = Protocol.STORE_TO_TOKEN + " ";
            for(int i=0; i<dstoreList.size(); i++){
                message = message + dstoreList.get(i) + " ";
            }
//            out.println(message);
            sendCommand(out,message);
            System.out.println("Command sent: " + message);
        }
    }

    private static synchronized void storeComplete(PrintWriter out, String fileName){
        try{
            index.addFileComplete(fileName);
            storingNow = false;
            String message = Protocol.STORE_COMPLETE_TOKEN;
//            out.println(message);
            sendCommand(out,message);
            storeWriterHash.remove(fileName);
            System.out.println("Command sent: " + message);
        } catch(Exception e){
            System.out.println("Error: " + e);
        }
    }
    private static synchronized int getPort(int index){
        return dstoreList.get(index);
    }



    private static synchronized void loadFile(PrintWriter out, int port, String fileName){
        int fileSize;
        String message;

        if(index.isFileStored(fileName)){
            fileSize = index.getFileSize(fileName);
            message = Protocol.LOAD_FROM_TOKEN + " " + port + " " + fileSize;
//            out.println(message);
            sendCommand(out,message);
            System.out.println("Command sent: " + message);

        } else{
            Errors.errorFileDoesNotExist(out);
        }
    }

    /**
     * Removes a file with a given name from a given Dstore
     * @param fileName File name
     * @param port Dstore port
     */
    private static synchronized void removeFile(String fileName, int port){
        removingNow = true;
        try{
            Socket dstoreSocket = new Socket(InetAddress.getLoopbackAddress(),port);
            PrintWriter dstoreWriter = new PrintWriter(dstoreSocket.getOutputStream(), true);
            String message = Protocol.REMOVE_TOKEN + " " + fileName;
//            dstoreWriter.println(message);
            sendCommand(dstoreWriter,message);
            System.out.println("Command sent to  " + port + ": " + message);
        } catch(Exception e){System.out.println("Error " + e);}

    }

    private static synchronized void removeComplete(PrintWriter out, String fileName){
        index.removeFileComplete(fileName);
        removingNow = false;
        removeWriterHash.remove(fileName);
        String message = Protocol.REMOVE_COMPLETE_TOKEN;
//        out.println(message);
        sendCommand(out,message);
        System.out.println("Command sent: " + message);
    }

    private static synchronized void startRebalance(Integer timeout){
        ScheduledExecutorService executor = Executors.newScheduledThreadPool(1);
        executor.scheduleAtFixedRate(() -> rebalance(), timeout, timeout, TimeUnit.MILLISECONDS);
    }

    private static synchronized void rebalance(){

//        while(storingNow || removingNow);

        if(!rebalancing){
            rebalancing = true;
            dstoreRebalanceHash = new Hashtable<>();

            if(!enoughDstores()){
                System.out.println(Protocol.ERROR_NOT_ENOUGH_DSTORES_TOKEN);
                return;
            }

            for(Integer port: dstoreList){
                getDstoreFiles(port);
            }

            for(Integer port : dstoreList){
                System.out.println(port + " " + dstoreRebalanceHash.get(port));
            }
        } else{
            System.out.println("bruh");
        }

        rebalancing = false;

    }

    private static synchronized void getDstoreFiles(Integer port){
        try{
            Socket dstoreSocket = new Socket(InetAddress.getLoopbackAddress(), port);
            PrintWriter writer = new PrintWriter(dstoreSocket.getOutputStream(), true);
            String message = Protocol.LIST_TOKEN;
//            writer.println(message);
            sendCommand(writer,message);
            System.out.println("Command sent: " + message);
        } catch(Exception e){System.out.println("Error " + e);}
    }

    private static synchronized void rebalanceList(Integer port, String line){
        String message[] = line.split(" ");
        ArrayList<String> tempList = new ArrayList<>();
        if(message.length > 1){
            for(int i=1; i<message.length; i++){
                tempList.add(message[i]);
            }
        }
        dstoreRebalanceHash.put(port,tempList);
    }

}
