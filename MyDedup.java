import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import javax.swing.text.StyledEditorKit;

public class MyDedup{
    static int sha1_size = 28; //length of base64-encoded SHA1-fingerprint
    static int max_container_size = 1048576; // 1 MiB, max container size
    static int curr_chunk_size = 0; // chunk size in bytes
    static int window_size;
    static String delimiter = "@";
    static String delimiter2 = "#";

    static byte[] b_array; // byte array for storing current chunk
    static byte[] prev_wind; //for storing previous window bytes
    static ByteBuffer container_buffer = ByteBuffer.allocate(1048576); // buffer for storing container
    static HashMap<String, int[]> index_mapping;  //for in-memory store of index
    static ArrayList<String> fpList = new ArrayList<String>(); // file recipe, check: what to do if file already existed?
    static int wind_start_index = 0; //start index of window relative to chunk start position
    static MessageDigest SHA1_md;
    static String file_path;
    static ArrayList<String> file_recipe = new ArrayList<String>();
    static Set<String> file_recipe_set = new HashSet<String>();

    // stats
    static int total_containers = 0;
    static int total_store_files = 0;
    static int prededup_chunks = 0;
    static int unique_chunks = 0;
    static int prededup_bytes = 0;
    static int unique_bytes = 0;

    static boolean debug = false;
    static boolean print_rfp = false;
    static boolean print_byte = false;
    static boolean print_chunk_size = false;

    public static void main(String args[]) throws Exception{
        SHA1_md = MessageDigest.getInstance("SHA-1");

        if(args[0].equals("upload"))
            upload(Integer.parseInt(args[1]), Integer.parseInt(args[2]), 
                Integer.parseInt(args[3]), Integer.parseInt(args[4]), args[5]);
        else if(args[0].equals("download"))
            download(args[1], args[2]);
        else
            System.exit(1);
    }

    // return -1, if n is not a power of two
    // return k, if n == 2^k
    public static int getLogN(int n){
        if(n == 0) return -1;
        int k = 0;
        while(n != 1){
            if(n % 2 == 0){
                k += 1;
                n = n/2;
            }else{
                return -1;
            }
        }
        return k;
    }

    // return n^p mod q, where q is a power of two
    public static long modular_expo(long n, int p, int q){
        long prod = 1;
        for(int j=0 ; j<p; j+=1){
            // check AND btw long and int?
            prod = (prod * (n & (q-1)) & (q-1));
        }
        return prod;
    }

    // return n mod q, where q is a power of two
    public static long mod_q(int n, int q){
        return n & (q-1);
    }

    // return n mod q, where q is a power of two
    public static long mod_q(long n, int q){
        return n & (q-1);
    }

    public static String bytesArrToString(byte[] bArr){
        return Base64.getEncoder().encodeToString(bArr);
    }

    public static byte[] stringToBytesArr(String s){
        return Base64.getDecoder().decode(s);
    }

    public static void getIndex(boolean download){
        index_mapping = new HashMap<String, int[]>();

        File index_file = new File("mydedup.index");
        try{
            // file exists
            BufferedReader br = new BufferedReader(new FileReader(index_file));
            parseMetadataStr(br.readLine());

            String line;
            while ((line = br.readLine()) != null) {
                String[] p = line.split(delimiter);
                String fp = p[0];
                int container_id = Integer.parseInt(p[1]);
                int chunk_offset_start = Integer.parseInt(p[2]);
                int chunk_size = Integer.parseInt(p[3]);
                int[] arr = new int[] {container_id, chunk_offset_start, chunk_size};

                if(download & file_recipe_set.contains(fp)){
                    index_mapping.put(fp, arr);
                }

                if(!download){
                    index_mapping.put(fp, arr);
                }
            }
        }catch(Exception e){

        }
    }

    public static String getMetadataStr(){
        return total_containers + delimiter + total_store_files + delimiter
            + prededup_chunks + delimiter + unique_chunks + delimiter + prededup_bytes + delimiter + unique_bytes;
    }

    public static void parseMetadataStr(String s){
        String[] arr = s.split(delimiter);
        total_containers = Integer.parseInt(arr[0]);
        total_store_files = Integer.parseInt(arr[1]);
        prededup_chunks = Integer.parseInt(arr[2]);
        unique_chunks = Integer.parseInt(arr[3]);
        prededup_bytes = Integer.parseInt(arr[4]);
        unique_bytes = Integer.parseInt(arr[5]);
    }

    public static void saveIndex() throws Exception{
        File index_file = new File("mydedup.index");
        Files.deleteIfExists(index_file.toPath());

        if(!index_file.createNewFile()){
            System.exit(1);
        }

        FileWriter fw = new FileWriter(index_file);
        fw.write(getMetadataStr());
        fw.write(System.lineSeparator());
        for(Map.Entry<String, int[]> e: index_mapping.entrySet()){
            int[] pair = e.getValue();
            int container_id = pair[0];
            int chunk_offset_start = pair[1];
            int chunk_size = pair[2];

            fw.write(e.getKey() + delimiter + container_id + delimiter + chunk_offset_start + delimiter + chunk_size);
            fw.write(System.lineSeparator());
        }
        fw.close();
    }

    public static void getFileRecipe(String file_to_download){

        File f = new File("mydedup.file_recipe");
        try{
            BufferedReader br = new BufferedReader(new FileReader(f));
            String line;
            while((line = br.readLine()) != null){
                String[] fps = line.split(delimiter);
                String fname = fps[0];
                
                if(fname.equals(file_to_download)){
                    for(int i =1 ; i<fps.length; i+=1){
                        String fp = fps[i].split(delimiter2)[0];
                        file_recipe.add(fps[i]);
                        file_recipe_set.add(fp);
                    }
                    return;
                }
            }
        }catch(Exception e){
        }
    }

    public static void appendToFileRecipe() throws Exception{

        File f = new File("mydedup.file_recipe");
        if(!f.exists()){
            if(!f.createNewFile()){
                System.out.println("cannot create file recipe");
                System.exit(1);
            }
        }

        RandomAccessFile stream = new RandomAccessFile("mydedup.file_recipe", "rw");
        stream.seek(stream.length());
        FileChannel channel = stream.getChannel();


        ByteBuffer buffer = ByteBuffer.allocate(System.lineSeparator().length() + file_path.length() + fpList.size()*(sha1_size + 4));

        if(total_store_files != 1){
            // debugPrint("put line sep");
            buffer.put(System.lineSeparator().getBytes());
        }

        buffer.put(file_path.getBytes());
        int count = 1;
        String prev = fpList.get(0);
        for(int i=1 ; i<fpList.size(); i+=1){
            if(prev.equals(fpList.get(i))){
                count += 1;
            }else{
                buffer.put((delimiter + prev + delimiter2 + count).getBytes());
                count = 1;
                prev = fpList.get(i);
            }
        }
        buffer.put((delimiter + prev + delimiter2 + count).getBytes());
        
        buffer.flip();
        channel.write(buffer);
        stream.close();
        channel.close();
    }

    public static String getSHA1(byte[] bArr){
        SHA1_md.update(bArr, 0, curr_chunk_size);
        return Base64.getEncoder().encodeToString(SHA1_md.digest());
    }

    public static void debugPrint(String s){
        if(debug) System.out.println(s);
    }
    public static void flushContainer() throws Exception{
        // if container is empty, return
        if(container_buffer.position() == 0) return;

        // final long startTime = System.currentTimeMillis();
        total_containers += 1;

        File f = new File("data/" + Integer.toString(total_containers));

        // create "data" directory and create the container file
        if (!f.getParentFile().exists()) 
            f.getParentFile().mkdirs();
        if(!f.exists()){
            f.createNewFile();
        }

        // write to the container file
        FileChannel fc = new FileOutputStream(f).getChannel();
        container_buffer.flip(); //reset position to zero, reset limit to current position, change to read mode
        fc.write(container_buffer);

        // clear container and close writer
        fc.close();
        container_buffer.clear(); //reset position to zero, limit to capacity, change to write mode

        // debugPrint("flushContainer time: " + (System.currentTimeMillis() - startTime));
    }

    public static void saveChunk(boolean last) throws Exception{
        printChunkSize(""+curr_chunk_size);

        if(!last){
            for(int j=0 ;j < window_size;j+=1){
                prev_wind[j] = b_array[curr_chunk_size - window_size + j];
            }
        }

        // debugPrint("save chunk");
        String fingerprint = getSHA1(b_array);
        prededup_chunks += 1;
        // debugPrint("chunk size: " + curr_chunk_size);
        prededup_bytes += curr_chunk_size;

        if(!index_mapping.containsKey(fingerprint)){ // chunk not exist in indexing
            // if container size will be full
            unique_chunks += 1;
            unique_bytes += curr_chunk_size;
            if(container_buffer.position() + curr_chunk_size > max_container_size){
                // save container
                flushContainer();
            }

            // add chunk to indexing
            index_mapping.put(fingerprint, new int[]{total_containers + 1, container_buffer.position(), curr_chunk_size});
            for(int j=0; j<curr_chunk_size; j+=1){
                container_buffer.put(b_array[j]);
            }
        }

        fpList.add(fingerprint); //add to file_recipe
        curr_chunk_size = 0; // restart curr_chunk_size
        wind_start_index = 0; // restart
    }

    public static void reportStats(){
        System.out.println("Total number of files that have been stored: " + total_store_files);
        System.out.println("Total number of pre-deduplicated chunks in storage: " + prededup_chunks);
        System.out.println("Total number of unique chunks in storage: " + unique_chunks);
        System.out.println("Total number of bytes of pre-deduplicated chunks in storage: " + prededup_bytes);
        System.out.println("Total number of bytes of unique chunks in storage: " + unique_bytes);
        System.out.println("Total number of containers in storage: " + total_containers);
        System.out.println("Deduplication ratio: " + (double) prededup_bytes/unique_bytes);
    }

    public static void printByte(String s){
        if(debug && print_byte) System.out.println(s);
    }

    public static void printRFP(String s){
        if(debug & print_rfp) System.out.println(s);
    }

    public static void printChunkSize(String s){
        if(debug & print_chunk_size) System.out.println(s);
    }

    public static void upload(int min_chunk, int avg_chunk, int max_chunk, long d, String file_to_upload) throws Exception{
        getIndex(false);

        file_path = file_to_upload;
        
        int log_min_chunk = getLogN(min_chunk);
        if(log_min_chunk == -1){
            System.out.println("min chunk not power of 2");
            System.exit(1);
        }

        int log_avg_chunk = getLogN(avg_chunk);
        if(log_avg_chunk == -1){
            System.out.println("avg chunk not power of 2");
            System.exit(1);
        }

        int log_max_chunk = getLogN(max_chunk);
        if(log_max_chunk == -1){
            System.out.println("max chunk not power of 2");
            System.exit(1);
        }

        // debugPrint("min_chunk: " + min_chunk);
        // debugPrint("avg_chunk: " + avg_chunk);
        // debugPrint("max_chunk: " + max_chunk);
        // debugPrint("d: " + d);

        window_size = min_chunk;
        b_array = new byte[max_chunk]; //byte array for storing current chunk
        prev_wind = new byte[window_size];

        // check if upload file exists
        File upload_file = new File(file_to_upload);
        if(!upload_file.exists()){
            System.out.println("upload file does not exist");
            System.exit(1);
        }

        // read from upload file
        // https://stackoverflow.com/questions/9093888/fastest-way-of-reading-relatively-huge-byte-files-in-java
        final FileChannel channel = new FileInputStream(upload_file).getChannel();
        MappedByteBuffer file_buffer = channel.map(FileChannel.MapMode.READ_ONLY, 0, channel.size());

        // chunking
        // int i = 0; // window pointer
        long rabin_fp = 0;
        
        long x = modular_expo(d, window_size - 1, avg_chunk);

        // final long startTime = System.currentTimeMillis();

        // first window
        boolean first_wind = true;

        while(file_buffer.hasRemaining()){
            // debugPrint("total_containers: " + total_containers);
            byte b;
            // add to chunk until achieve window_size
            while(curr_chunk_size < window_size){
                wind_start_index = 0;

                try{                    
                    b = file_buffer.get(); // throws exception when buffer is empty
                    printByte(""+b);
                }catch(Exception e){
                    break;
                }

                if(first_wind){
                    // calc rabin fingerprint
                    rabin_fp = mod_q(rabin_fp * d, avg_chunk);
                    rabin_fp = mod_q(rabin_fp + mod_q(b, avg_chunk), avg_chunk);
                }else{
                    long y = mod_q(x * mod_q(prev_wind[curr_chunk_size], avg_chunk), avg_chunk);
                    rabin_fp = mod_q(mod_q(mod_q(rabin_fp - y, avg_chunk) * mod_q(d, avg_chunk), avg_chunk) + b, avg_chunk);
                }
                printRFP(""+rabin_fp);
                b_array[curr_chunk_size] = b;
                curr_chunk_size += 1;
                // i += 1;
            }
            first_wind = false;

            // debugPrint("rabin_fp: " + rabin_fp);


            // bug1 when rfp is 0 => try to save but file size is max
            // if critical point is found
            if((rabin_fp & (avg_chunk -1)) == 0){
                // debugPrint("c size = " + curr_chunk_size);
                saveChunk(false);
                // debugPrint(""+rabin_fp);
            }else{
                // check chunk_size exceeds maximum
                if(curr_chunk_size + 1 > max_chunk){
                    // debugPrint(""+curr_chunk_size);
                    saveChunk(false);
                    // debugPrint(""+rabin_fp);
                }else{
                    try{
                        b = file_buffer.get(); // throws exception when buffer is empty
                        printByte(""+b);
                    }catch(Exception e){
                        break;
                    }

                    b_array[curr_chunk_size] = b;
                    curr_chunk_size += 1;
                    long y = mod_q(x * mod_q(b_array[wind_start_index], avg_chunk), avg_chunk);
                    rabin_fp = mod_q(mod_q(mod_q(rabin_fp - y, avg_chunk) * mod_q(d, avg_chunk), avg_chunk) + b, avg_chunk);

                    printRFP(""+rabin_fp);

                    // move window
                    wind_start_index += 1;
                }
            }   
        }

        // debugPrint("while-loop time: " + (System.currentTimeMillis() - startTime));

        if(curr_chunk_size != 0) saveChunk(true);
        // debugPrint(""+rabin_fp);
        flushContainer();

        total_store_files += 1;

        // final long startTime2 = System.currentTimeMillis();
        saveIndex();
        // debugPrint("save index: " + (System.currentTimeMillis() - startTime2));

        // final long startTime3 = System.currentTimeMillis();
        appendToFileRecipe();
        // debugPrint("append to file_recipe: " + (System.currentTimeMillis() - startTime3));

        channel.close();

        reportStats();
    }

    public static void download(String file_to_download, String local_file_name) throws Exception{
        getFileRecipe(file_to_download);
        getIndex(true);
        
        file_path = file_to_download;

        if(file_recipe_set.isEmpty()){
            System.out.println("download file does not exist");
            System.exit(1);
        }

        File f = new File(local_file_name);
        Files.deleteIfExists(f.toPath());
        
        if(local_file_name.contains("/")){
            String dirPath = local_file_name.substring(0, local_file_name.lastIndexOf("/")); 
            (new File(dirPath)).mkdirs();
        }
        if(!f.createNewFile()){
            System.out.println("cannot write a new file");
            System.exit(1);
        }

        FileOutputStream fos = new FileOutputStream(f);

        // TODO: can parallelize here
        for(String fps: file_recipe){
            String[] pair = fps.split(delimiter2);
            String fp = pair[0];
            int count = Integer.parseInt(pair[1]);
            if(!index_mapping.containsKey(fp)){
                System.out.println("cannot find chunk: " + fp);
                System.exit(1);
            }

            int container_id = index_mapping.get(fp)[0];
            int chunk_offset_start = index_mapping.get(fp)[1];
            int chunk_size = index_mapping.get(fp)[2];
            
            File container_file = new File("data/" + container_id);
            if(!container_file.exists()){
                System.out.println("container not exist");
                System.exit(1);
            }

            RandomAccessFile fr = new RandomAccessFile(container_file, "r");
            fr.seek(chunk_offset_start);
            
            byte[] cbuff = new byte[chunk_size];
            try{
                fr.read(cbuff, 0, chunk_size);
            }catch(Exception e){
                e.printStackTrace(System.out);
                System.out.println("error on reading from container " + container_id + ", offset = " 
                    + chunk_offset_start + ", size = " + chunk_size);
                System.exit(1);
            }
            for(int j = 0 ; j < count ; j += 1){
                fos.write(cbuff);
            }
            fr.close();
        }

        fos.close();
    }
}


// problem list
//  1. (done) java.lang.OutOfMemoryError when tested with test4-1gb.mp4
//  2. (done) appendToFileRecipe takes a long time which can be paralleled: https://stackoverflow.com/questions/6206472/what-is-the-best-way-to-write-to-a-file-in-a-parallel-thread-in-java
//  3. upload and download based on file path not file name