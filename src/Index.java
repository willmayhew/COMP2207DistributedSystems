import java.util.ArrayList;
import java.util.HashMap;

public class Index {

    //(File name, File state)
    HashMap<String,Boolean> fileStates = new HashMap<>();
    //(File name, File size)
    HashMap<String,Integer> fileSizes = new HashMap<>();

    /**
     * Updates the state of a file
     * @param fileName File name
     * @param state State to be updated to
     */
    public void updateState(String fileName, boolean state){
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
     * @param fileName File name
     * @param fileSize File size
     */
    public void addFile(String fileName, Integer fileSize){
        fileStates.put(fileName,false);
        fileSizes.put(fileName,fileSize);
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

}
