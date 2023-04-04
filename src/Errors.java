import java.io.PrintWriter;

public class Errors {

    public static void errorNotEnoughDstores(PrintWriter out){
        out.println("ERROR_NOT_ENOUGH_DSTORES");
        System.out.println("Command sent: ERROR_NOT_ENOUGH_DSTORES");
    }

    public static void errorFileAlreadyExists(PrintWriter out){
        out.println("ERROR_FILE_ALREADY_EXISTS");
        System.out.println("Command sent: ERROR_FILE_ALREADY_EXISTS");
    }

    public static void errorFileDoesNotExist(PrintWriter out){
        out.print("ERROR_FILE_DOES_NOT_EXIST");
        System.out.println("Command sent: ERROR_FILE_DOES_NOT_EXIST");
    }

    public static void errorLoad(PrintWriter out){
        out.println("ERROR_LOAD");
        System.out.println("Command sent: ERROR_LOAD");
    }

}
