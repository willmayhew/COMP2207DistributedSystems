import java.io.PrintWriter;

public class Errors {

    public static synchronized void errorNotEnoughDstores(PrintWriter out){
        String error = Protocol.ERROR_NOT_ENOUGH_DSTORES_TOKEN;
        out.println(error);
        out.flush();
        System.out.println("Command sent: " + error);
    }

    public static synchronized void errorFileAlreadyExists(PrintWriter out){
        String error = Protocol.ERROR_FILE_ALREADY_EXISTS_TOKEN;
        out.println(error);
        out.flush();
        System.out.println("Command sent: " + error);
    }

    public static synchronized void errorFileDoesNotExist(PrintWriter out){
        String error = Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN;
        out.println(error);
        out.flush();
        System.out.println("Command sent: " + error);
    }

    public static synchronized void errorLoad(PrintWriter out){
        String error = Protocol.ERROR_LOAD_TOKEN;
        out.println(error);
        out.flush();
        System.out.println("Command sent: " + error);
    }

}
