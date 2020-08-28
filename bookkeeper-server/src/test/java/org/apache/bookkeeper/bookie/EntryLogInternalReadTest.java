package org.apache.bookkeeper.bookie;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.conf.TestBKConfiguration;
import org.apache.bookkeeper.util.DiskChecker;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.apache.bookkeeper.util.IOUtils.createTempDir;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class EntryLogInternalReadTest {

    //ByteBuf bb = Unpooled.buffer(length);
    /*
    ByteBuf buf = newEntryLogger.readEntry(ledgerId, 0L, entry0Position);
        long readLedgerId = buf.readLong();
        long readEntryId = buf.readLong();
        Assert.assertEquals("LedgerId", ledgerId, readLedgerId);
        Assert.assertEquals("EntryId", 0L, readEntryId);

        buf = newEntryLogger.readEntry(ledgerId, 1L, entry1Position);
        readLedgerId = buf.readLong();
        readEntryId = buf.readLong();
        Assert.assertEquals("LedgerId", ledgerId, readLedgerId);
        Assert.assertEquals("EntryId", 1L, readEntryId);
     */
    //TEST PER public ByteBuf internalReadEntry(long ledgerId, long entryId, long location, boolean validateEntry)internalReadEntry

    private long ledgerId;
    private long entryId;
    private long location;
    private boolean validateEntry;
    private ByteBuf expectedRes;
    private ServerConfiguration conf;
    private LedgerDirsManager dirsMgr;
    private File curDir;

    private EntryLogger entryLogger;

    public EntryLogInternalReadTest(){

    }

    /*
    public EntryLogInternalReadTest (long ledgerId, long entryId, long location,  boolean validateEntry){

        this.ledgerId=ledgerId;
        this.entryId=entryId;
        this.location=location;
        this.validateEntry=validateEntry;


    }

     */

    final List<File> tempDirs = new ArrayList<File>();
    final List<EntryLogger> list = new ArrayList<>();


    private static ByteBuf generateEntry(long ledger, long entry) {
        byte[] data = generateDataString(ledger, entry).getBytes();
        ByteBuf bb = Unpooled.buffer(8 + 8 + data.length);
        bb.writeLong(ledger);
        bb.writeLong(entry);
        bb.writeBytes(data);
        return bb;
    }

    private static String generateDataString(long ledger, long entry) {
        return ("ledger-" + ledger + "-" + entry);
    }


    @Before
    public void setup() throws IOException {

        //creazione di una cartella root, consigurazione del server e del LedgerDirsManager
        //creazione di un EntryLogger (per le entry del ledger)

         File rootDir=null;

        try {
            rootDir = createTempDir("bkTest", ".dir");
        } catch (IOException e) {
            e.printStackTrace();
        }
        curDir = Bookie.getCurrentDirectory(rootDir);
        try {
            Bookie.checkDirectoryStructure(curDir);
        } catch (IOException e) {
            e.printStackTrace();
        }
        conf = TestBKConfiguration.newServerConfiguration();
        dirsMgr = new LedgerDirsManager(
                conf,
                new File[] { rootDir },
                new DiskChecker(
                        conf.getDiskUsageThreshold(),
                        conf.getDiskUsageWarnThreshold()));
        try {
            entryLogger = new EntryLogger(conf, dirsMgr);
        } catch (IOException e) {
            e.printStackTrace();
        }

        EntryLogManagerForSingleEntryLog elm = (EntryLogManagerForSingleEntryLog) entryLogger.getEntryLogManager();
        elm.createNewLog(10L);

        // add the first entry will trigger file creation


        System.out.println("-------current log id = "+elm.getCurrentLogId());



        System.out.println("-------------OK-------");

    }

    @Test
    public void testMissingLogId() throws Exception {
        // create some entries
        int numLogs = 3;
        int numEntries = 10;
        long[][] positions = new long[2 * numLogs][];
        for (int i = 0; i < numLogs; i++) {
            positions[i] = new long[numEntries];

            EntryLogger logger = new EntryLogger(conf, dirsMgr);
            for (int j = 0; j < numEntries; j++) {
                positions[i][j] = logger.addEntry((long) i, generateEntry(i, j).nioBuffer());
            }
            logger.flush();
            logger.shutdown();
        }
        // delete last log id
        File lastLogId = new File(curDir, "lastId");
        lastLogId.delete();

        // write another entries
        for (int i = numLogs; i < 2 * numLogs; i++) {
            positions[i] = new long[numEntries];

            EntryLogger logger = new EntryLogger(conf, dirsMgr);
            for (int j = 0; j < numEntries; j++) {
                positions[i][j] = logger.addEntry((long) i, generateEntry(i, j).nioBuffer());
            }
            logger.flush();
            logger.shutdown();
        }

        EntryLogger newLogger = new EntryLogger(conf, dirsMgr);
        for (int i = 0; i < (2 * numLogs + 1); i++) {
            File logFile = new File(curDir, Long.toHexString(i) + ".log");
            assertTrue(logFile.exists());
        }
        for (int i = 0; i < 2 * numLogs; i++) {
            for (int j = 0; j < numEntries; j++) {
                String expectedValue = "ledger-" + i + "-" + j;
                ByteBuf value2 = newLogger.readEntry(i, j, positions[i][j]);
                ByteBuf value = newLogger.internalReadEntry(i,j,positions[i][j],true);
                long ledgerId = value.readLong();
                long entryId = value.readLong();
                byte[] data = new byte[value.readableBytes()];
                value.readBytes(data);
                value.release();
                System.out.println("====VALORI: ledgerId= "+ledgerId+"   entryId= "+entryId+"   data= "+data.toString());

                assertEquals(i, ledgerId);
                assertEquals(j, entryId);
                assertEquals(expectedValue, new String(data));
            }
        }
    }





    @After
    public void tearDown() throws Exception {
        if (null != this.entryLogger) {
            entryLogger.shutdown();
        }

        for (File dir : tempDirs) {
            FileUtils.deleteDirectory(dir);
        }
        tempDirs.clear();
    }

    @Test
    public void testStupido(){
        assertEquals(2,2);
    }


}
