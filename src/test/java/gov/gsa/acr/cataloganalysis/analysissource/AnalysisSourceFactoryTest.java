package gov.gsa.acr.cataloganalysis.analysissource;

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
@ContextConfiguration(classes ={AnalysisSourceFactory.class})
@TestPropertySource(locations="classpath:application-junit.properties")
class AnalysisSourceFactoryTest {

    @MockBean
    AnalysisSourceXsb xsbSourceSftpFiles;

    @MockBean
    AnalysisSourceLocal xsbSourceLocalFiles;

    @MockBean
    AnalysisSourceS3 xsbSourceS3Files;

    @Autowired
    AnalysisSourceFactory analysisSourceFactory;

    @Test
    void testXsbSourceNullTrigger() {
        NullPointerException thrown = assertThrows (NullPointerException.class, () -> analysisSourceFactory.xsbSource(null));
        assertTrue(thrown.getMessage().matches(".*trigger.* is null"));
    }

    @Test
    void testXsbSourceNoSourceType() {
        Trigger trigger = new Trigger();
        NullPointerException thrown = assertThrows (NullPointerException.class, () -> analysisSourceFactory.xsbSource(trigger));
        log.info(thrown.getMessage());
        assertTrue(thrown.getMessage().matches(".*SourceType.* is null"));
    }

    @Test
    void testXsbSourceTypeSFTP() {
        Trigger trigger = new Trigger();
        trigger.setSourceType(Trigger.AnalysisSourceType.XSB);
        assertEquals(xsbSourceSftpFiles, analysisSourceFactory.xsbSource(trigger));
    }

    @Test
    void testXsbSourceTypeLOCAL() {
        Trigger trigger = new Trigger();
        trigger.setSourceType(Trigger.AnalysisSourceType.LOCAL);
        assertEquals(xsbSourceLocalFiles, analysisSourceFactory.xsbSource(trigger));
    }

    @Test
    void testXsbSourceTypeS3() {
        Trigger trigger = new Trigger();
        trigger.setSourceType(Trigger.AnalysisSourceType.S3);
        assertEquals(xsbSourceS3Files, analysisSourceFactory.xsbSource(trigger));
    }
}