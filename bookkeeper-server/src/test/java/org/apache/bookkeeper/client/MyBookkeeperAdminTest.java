package org.apache.bookkeeper.client;


import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.assertEquals;


//uso extends perche classe BookkeeperAdmin rappresenta l'admin client per un cluster

@RunWith(value = Parameterized.class)
public class MyBookkeeperAdminTest extends BookKeeperClusterTestCase {

    private BookKeeper.DigestType digest = BookKeeper.DigestType.CRC32;

    //format -> metodo che formatta (inizializza) i metadati dell'oggetto Bookkeeper in zook.

    private boolean isInteractive;
    private boolean force;
    private boolean serverConfIsValid;
    private boolean expectedRes;
    private int countTest;

    private static final int numOfLedgers = 2;
    private static final int numBookies = 2;




    //public MyBookkeeperAdminTest(boolean expectedRes, ServerConfiguration serverConf, boolean isInteractive, boolean force) {
    public MyBookkeeperAdminTest(boolean expectedRes, boolean serverConfIsValid, boolean isInteractive, boolean force, int countTest) {

        super(numBookies);

        setAutoRecoveryEnabled(true);

        this.expectedRes=expectedRes;
        this.serverConfIsValid=serverConfIsValid;

        this.isInteractive=isInteractive;
        this.force=force;
        this.countTest=countTest;

    }


    @Parameterized.Parameters
    public static Collection<?> getTestParameters() {
        return Arrays.asList(new Object[][] {

                {true, true, false, true,0},        //expectedRes, serverConf, isInteractive, force, countTest
                {false, true, false, false,1},
                //ora non so quanto expectedRes valga, può essere TRUE O FALSE
                {true, true, true, true,2},         //expectedRes = TRUE
                {false, true, true, true,3},         //expectedRes = FALSE
                {true, true, true, false,4},         //expectedRes = TRUE
                {false, true, true, false,5},         //expectedRes = FALSE

                {false, null, false, false,6},         //serverConf = null , per ogni force e isInteractive
                // mi aspetto sempre expectedRes = FALSE


        });
    }


    //vedi pure num di ledgers già scritti che devono essere cancellati quando bookieAdmin è formattato
    //ledger id devono ripartire da 0


    @Before
    public void setup() throws InterruptedException, BKException, IOException {

        ClientConfiguration clientConf = new ClientConfiguration();
        clientConf.setMetadataServiceUri(zkUtil.getMetadataServiceUri());

        try (BookKeeper bk = new BookKeeper(clientConf)){
            Set<Long> ledgersId = new HashSet<>();


            for (int i=0;i<numBookies;i++){

                try (LedgerHandle lh = bk.createLedger(numOfLedgers,numOfLedgers,digest,"L".getBytes())) {
                    ledgersId.add(lh.getId());
                    lh.addEntry("000".getBytes());
                }
            }

        }


    }

    @After
    public void tearDown() throws Exception{
        super.tearDown();
    }

    @Test
    public void formatTest() throws InterruptedException, BKException, IOException {
        boolean res;

        System.out.println("======================================= STO NEL TEST !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");
        if (isInteractive){
            //deve rispondere una volta si e una no
            if ( (countTest%2)==0 ) {

                System.setIn(new ByteArrayInputStream("y\n".getBytes(),0,2));
            }
            else{
                System.setIn(new ByteArrayInputStream("n\n".getBytes(),0,2));
            }

            System.out.println("countTest= " + countTest+"    resto= "+countTest%2);
        }

        try {

            if (serverConfIsValid){
                res = BookKeeperAdmin.format( baseConf, isInteractive, force);
                System.out.println("res= " + res + "   expectedRes= " + expectedRes);
                assertEquals(expectedRes, res);
            }
            else{
                res = BookKeeperAdmin.format( null, isInteractive, force);
                System.out.println("res= " + res + "   expectedRes= " + expectedRes);
                assertEquals(expectedRes, res);

            }

        } catch (Exception e){
            e.printStackTrace();
        }
        /*
        try (BookKeeperAdmin bA = new BookKeeperAdmin(zkUtil.getZooKeeperConnectString())) {

            System.out.println("============STO NEL TRY !!!!!!");
            res = BookKeeperAdmin.format(null, isInteractive, force);
            System.out.println("res= "+res+"   expectedRes= "+expectedRes);
            assertEquals(expectedRes,res);


        } catch (Exception e) {
            e.printStackTrace();
        }
  */

    }


}






/*

    import static java.nio.charset.StandardCharsets.UTF_8;
    import static org.apache.bookkeeper.util.BookKeeperConstants.AVAILABLE_NODE;
    import static org.apache.bookkeeper.util.BookKeeperConstants.READONLY;
    import static org.hamcrest.Matchers.is;
    import static org.junit.Assert.assertEquals;
    import static org.junit.Assert.assertFalse;
    import static org.junit.Assert.assertNotNull;
    import static org.junit.Assert.assertThat;
    import static org.junit.Assert.assertTrue;
    import static org.junit.Assert.fail;

    import com.google.common.net.InetAddresses;
    import java.io.File;
    import java.util.ArrayList;
    import java.util.Collection;
    import java.util.HashSet;
    import java.util.Iterator;
    import java.util.List;
    import java.util.Objects;
    import java.util.Random;
    import java.util.Set;
    import java.util.concurrent.CompletableFuture;
    import java.util.concurrent.CountDownLatch;
    import java.util.concurrent.ExecutionException;
    import java.util.concurrent.atomic.AtomicBoolean;
    import java.util.concurrent.atomic.AtomicInteger;

    import org.apache.bookkeeper.bookie.Bookie;
    import org.apache.bookkeeper.client.BookKeeper.DigestType;
    import org.apache.bookkeeper.client.api.LedgerMetadata;
    import org.apache.bookkeeper.common.component.ComponentStarter;
    import org.apache.bookkeeper.common.component.Lifecycle;
    import org.apache.bookkeeper.common.component.LifecycleComponent;
    import org.apache.bookkeeper.conf.ClientConfiguration;
    import org.apache.bookkeeper.conf.ServerConfiguration;
    import org.apache.bookkeeper.conf.TestBKConfiguration;
    import org.apache.bookkeeper.discover.BookieServiceInfo;
    import org.apache.bookkeeper.meta.UnderreplicatedLedger;
    import org.apache.bookkeeper.meta.ZkLedgerUnderreplicationManager;
    import org.apache.bookkeeper.meta.zk.ZKMetadataDriverBase;
    import org.apache.bookkeeper.net.BookieSocketAddress;
    import org.apache.bookkeeper.proto.BookieServer;
    import org.apache.bookkeeper.replication.ReplicationException.UnavailableException;
    import org.apache.bookkeeper.server.Main;
    import org.apache.bookkeeper.server.conf.BookieConfiguration;
    import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
    import org.apache.bookkeeper.util.AvailabilityOfEntriesOfLedger;
    import org.apache.bookkeeper.util.BookKeeperConstants;
    import org.apache.bookkeeper.util.PortManager;
    import org.apache.commons.io.FileUtils;
    import org.apache.zookeeper.CreateMode;
    import org.apache.zookeeper.ZooDefs.Ids;
    import org.junit.Assert;
    import org.junit.Test;
    import org.slf4j.Logger;
    import org.slf4j.LoggerFactory;

    public class MyBookkeeperAdminTest extends BookKeeperClusterTestCase {

        private static final Logger LOG = LoggerFactory.getLogger(BookKeeperAdminTest.class);
        private BookKeeper.DigestType digestType = BookKeeper.DigestType.CRC32;
        private static final String PASSWORD = "testPasswd";
        private static final int numBookies = 2;
        private final int lostBookieRecoveryDelayInitValue = 1800;


        public MyBookkeeperAdminTest() {
            super(numBookies);
            baseConf.setLostBookieRecoveryDelay(lostBookieRecoveryDelayInitValue);
            baseConf.setOpenLedgerRereplicationGracePeriod(String.valueOf(30000));
            setAutoRecoveryEnabled(true);
        }


        @Test
        public void testBookkeeperAdminFormatResetsLedgerIds() throws Exception {
            ClientConfiguration conf = new ClientConfiguration();
            conf.setMetadataServiceUri(zkUtil.getMetadataServiceUri());


        int numOfLedgers = 2;
        boolean res;
        try (BookKeeper bkc = new BookKeeper(conf)) {
            Set<Long> ledgerIds = new HashSet<>();
            for (int n = 0; n < numOfLedgers; n++) {
                try (LedgerHandle lh = bkc.createLedger(numBookies, numBookies, digestType, "L".getBytes())) {
                    ledgerIds.add(lh.getId());
                    lh.addEntry("000".getBytes());
                }
            }

            try (BookKeeperAdmin bkAdmin = new BookKeeperAdmin(zkUtil.getZooKeeperConnectString())) {
                res = bkAdmin.format(baseConf, false, true);
            }
            System.out.println("======================================= STO NEL TEST DOPO FORMAT!!!!!!!!!!!!!!!!!!");

            System.out.println("\n---->(deve venire true), res = "+res);

            System.out.println("\n---->ledgersIds =  = "+ledgerIds);
            for (int n = 0; n < numOfLedgers; n++) {
                try (LedgerHandle lh = bkc.createLedger(numBookies, numBookies, digestType, "L".getBytes())) {
                    lh.addEntry("000".getBytes());
                    assertTrue(ledgerIds.contains(lh.getId()));
                }
            }

        }
    }
}



 */

/*

import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.ByteArrayInputStream;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

@RunWith(value = Parameterized.class)
public class MyBookkeeperAdminTest extends BookKeeperClusterTestCase
{
    private BookKeeper.DigestType digestType = BookKeeper.DigestType.CRC32;
    private static final int numOfBookies = 2;

    private boolean hasValidServConf;
    private boolean isInteractive;
    private boolean isInteractiveYes;
    private boolean force;
    private boolean expectedResult;

    public MyBookkeeperAdminTest(boolean hasValidServConf, boolean isInteractive, boolean isInteractiveYes, boolean force, boolean expectedResult) {

        super(numOfBookies);


        this.hasValidServConf = hasValidServConf;
        this.isInteractive = isInteractive;
        this.isInteractiveYes = isInteractiveYes;
        this.force = force;
        this.expectedResult = expectedResult;




    }

    //Parametri in input
    @Parameterized.Parameters
    public static Collection<?> getParameters(){
        return Arrays.asList(new Object[][] {

                //serverConfig, isInteractive, isInteractiveYes, force, expectedresult
                {true, true, true, true, true},
                {true, true, false, false, false},
                {true, true, false, false, false},
                {false, true, false, false, false},
                {true, false, false, true, true},
                {true, false, false, true, true},
        });
    }

    @Before
    public void setUp() throws Exception {
        try {

            super.setUp();
        }catch (Exception e){ e.printStackTrace(); }

        try {
            ClientConfiguration conf = new ClientConfiguration();
            conf.setMetadataServiceUri(zkUtil.getMetadataServiceUri());


            int numOfLedgers = 2;
            try (BookKeeper bkc = new BookKeeper(conf)) {
                Set<Long> ledgerIds = new HashSet<>();
                for (int n = 0; n < numOfLedgers; n++) {
                    try (LedgerHandle lh = bkc.createLedger(numOfBookies, numOfBookies, digestType, "L".getBytes())) {
                        ledgerIds.add(lh.getId());
                        lh.addEntry("000".getBytes());
                    }
                }
            }
            catch( Exception e1){ e1.printStackTrace();}




        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    @After
    public void tearDown() throws Exception{
        super.tearDown();
    }


    @Test
    public void format() {



        if (isInteractive) {
            if (isInteractiveYes) System.setIn(new ByteArrayInputStream("y\n".getBytes(), 0, 2));
            else System.setIn(new ByteArrayInputStream("n\n".getBytes(), 0, 2));
        }

        boolean result;
        try{
            if(hasValidServConf) result = BookKeeperAdmin.format(baseConf, isInteractive, force);
            else result = BookKeeperAdmin.format(null, isInteractive, force);
            System.out.println("OK");
            System.out.println("====== result = "+result+"   baseConf = "+baseConf);

        } catch (Exception e) {
            result = false;
            e.printStackTrace();
        }

        Assert.assertEquals(result, expectedResult);


    }



}
*/


