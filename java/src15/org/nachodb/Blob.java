package org.nachodb;

import java.io.*;

/**
 * Interface to store/fetch large binary objects
 */
public interface Blob extends IPersistent, IResource { 
    /**
     * Get input stream. InputStream.availabe method can be used to get BLOB size
     * @return input stream with BLOB data
     */
    InputStream getInputStream();    

    /**
     * Get output stream to append data to the BLOB.
     * @return output srteam 
     */
    OutputStream getOutputStream();

    /**
     * Get output stream to append data to the BLOB.
     * @param multisession whether BLOB allows further appends of data or closing 
     * this output streat means that BLOB will not be changed any more. 
     * @return output srteam 
     */
    OutputStream getOutputStream(boolean multisession);
};