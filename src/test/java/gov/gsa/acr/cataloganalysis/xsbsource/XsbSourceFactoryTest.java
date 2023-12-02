package gov.gsa.acr.cataloganalysis.xsbsource;

import gov.gsa.acr.cataloganalysis.model.Trigger;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest
@Slf4j
@ContextConfiguration(classes ={XsbSourceFactory.class})
@TestPropertySource(locations="classpath:application-junit.properties")
class XsbSourceFactoryTest {

    @MockBean
    XsbSourceSftpFiles xsbSourceSftpFiles;

    @MockBean
    XsbSourceLocalFiles xsbSourceLocalFiles;

    @MockBean
    XsbSourceS3Files xsbSourceS3Files;

    @Autowired
    XsbSourceFactory xsbSourceFactory;

    @Test
    void testXsbSourceNullTrigger() {
        NullPointerException thrown = assertThrows (NullPointerException.class, () -> xsbSourceFactory.xsbSource(null));
        assertTrue(thrown.getMessage().matches(".*trigger.* is null"));
    }

    @Test
    void testXsbSourceNoSourceType() {
        Trigger trigger = new Trigger();
        NullPointerException thrown = assertThrows (NullPointerException.class, () -> xsbSourceFactory.xsbSource(trigger));
        log.info(thrown.getMessage());
        assertTrue(thrown.getMessage().matches(".*SourceType.* is null"));
    }

    @Test
    void testXsbSourceTypeSFTP() {
        Trigger trigger = new Trigger();
        trigger.setSourceType(Trigger.XsbSourceType.SFTP);
        assertEquals(xsbSourceSftpFiles, xsbSourceFactory.xsbSource(trigger));
    }

    @Test
    void testXsbSourceTypeLOCAL() {
        Trigger trigger = new Trigger();
        trigger.setSourceType(Trigger.XsbSourceType.LOCAL);
        assertEquals(xsbSourceLocalFiles, xsbSourceFactory.xsbSource(trigger));
    }

    @Test
    void testXsbSourceTypeS3() {
        Trigger trigger = new Trigger();
        trigger.setSourceType(Trigger.XsbSourceType.S3);
        assertEquals(xsbSourceS3Files, xsbSourceFactory.xsbSource(trigger));
    }
}