import java.util.ArrayList;
import java.util.HashMap;

public class Index {

    //(File name, File state)
    HashMap<String,String> fileStates = new HashMap<>();
    //(File name, File size)
    HashMap<String,Integer> fileSizes = new HashMap<>();


    /**
     * Updates the state of a file
     * @param fileName File name
     * @param state State to be updated to
     */
    public void updateState(String fileName, String state){
        fileStates.put(fileName,state);
    }

    /**
     * Checks if a file exists
     * @param fileName File name
     * @return File exists
     */
    public boolean fileExists(String fileName){
        return fileSizes.containsKey(fileName);
    }

    /**
     * Adds a file to the index
     * Updates the index to show a file is being added
     * @param fileName File name
     * @param fileSize File size
     */
    public void addFile(String fileName, Integer fileSize){
        fileStates.put(fileName,"store in progress");
        fileSizes.put(fileName,fileSize);
        System.out.println("Index Update: Storing " + fileName);
    }

    /**
     * Updates the index to show a file has been added
     * @param fileName File name
     */
    public void addFileComplete(String fileName){
        fileStates.put(fileName, "store complete");
        System.out.println("Index Update: " + fileName + " Store complete");
    }

    /**
     * Updates the index to show a file is being removed
     * @param fileName File name
     */
    public void removeFile(String fileName){
        fileStates.put(fileName, "remove in progress");
        System.out.println("Index Update: Removing " + fileName);
    }

    /**
     * Updates the index to show a file has been removed
     * @param fileName File name
     */
    public void removeFileComplete(String fileName){
        fileStates.put(fileName, "remove complete");
        System.out.println("Index Update: " + fileName + " Remove complete");
    }

    /**
     * Gets all file names in the index
     * @return List of file names
     */
    public ArrayList<String> getFileNames(){
        return new ArrayList<>(fileSizes.keySet());
    }

    /**
     * Gets the file size of a given file
     * @param fileName File name
     * @return File size
     */
    public int getFileSize(String fileName){
        return fileSizes.get(fileName);
    }

    public String getFileState(String fileName){
        return fileStates.get(fileName);
    }

    /**
     * Whether the file is busy or not
     * @param fileName File name
     * @return boolean for file is busy
     */
    public boolean fileBusy(String fileName){
        String fileState = getFileState(fileName);
        return fileState != null && (fileState.equals("remove in progress") || fileState.equals("store in progress"));
    }
}
