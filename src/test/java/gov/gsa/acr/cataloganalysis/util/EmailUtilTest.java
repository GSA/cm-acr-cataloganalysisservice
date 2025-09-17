package gov.gsa.acr.cataloganalysis.util;

import gov.gsa.acr.cataloganalysis.analysissource.AnalysisSourceXsb;
import gov.gsa.acr.cataloganalysis.repositories.XsbDataRepository;
import gov.gsa.acr.cataloganalysis.scheduler.ScheduledTasks;
import gov.gsa.acr.cataloganalysis.service.AnalysisDataProcessingService;
import gov.gsa.acr.cataloganalysis.service.XsbPpApiService;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.boot.test.mock.mockito.MockBeans;
import org.springframework.mail.SimpleMailMessage;
import org.springframework.mail.javamail.JavaMailSender;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.ContextConfiguration;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;


@Slf4j
@SpringBootTest
@ActiveProfiles("test")
@MockBeans({@MockBean(JavaMailSender.class)})
@ContextConfiguration(classes = {EmailUtil.class})
class EmailUtilTest {
    @Autowired
    private JavaMailSender emailSender;
    @Autowired
    private EmailUtil emailUtil;

    @Test
    void testSendEmail_DefaultAddr() {
        // Test data
        String from = "junit@test.com";
        String to = "test@junit.com";
        String subject = "Test Subject";
        String body = "Test Body";

        // Call the method
        emailUtil.sendEmail(subject, body);

        // Verify that the email was sent with correct parameters
        ArgumentCaptor<SimpleMailMessage> messageCaptor = ArgumentCaptor.forClass(SimpleMailMessage.class);
        verify(emailSender, times(1)).send(messageCaptor.capture());

        SimpleMailMessage sentMessage = messageCaptor.getValue();
        assertEquals(from, sentMessage.getFrom());
        assertEquals(to, sentMessage.getTo()[0]);
        assertEquals(subject, sentMessage.getSubject());
        assertEquals(body, sentMessage.getText());
    }



    @Test
    void testSendEmail() {
        // Test data
        String from = "test@example.com";
        String to = "recipient@example.com";
        String subject = "Test Subject";
        String body = "Test Body";

        // Call the method
        emailUtil.sendEmail(from, to, subject, body);

        // Verify that the email was sent with correct parameters
        ArgumentCaptor<SimpleMailMessage> messageCaptor = ArgumentCaptor.forClass(SimpleMailMessage.class);
        verify(emailSender, times(1)).send(messageCaptor.capture());

        SimpleMailMessage sentMessage = messageCaptor.getValue();
        assertEquals(from, sentMessage.getFrom());
        assertEquals(to, sentMessage.getTo()[0]);
        assertEquals(subject, sentMessage.getSubject());
        assertEquals(body, sentMessage.getText());
    }


    @Test
    @DisplayName("Test sendEmail method handles errors appropriately")
    void testSendEmail_Error() {
        // Test data
        String from = "test@example.com";
        String to = "recipient@example.com";
        String subject = "Test Subject";
        String body = "Test Body";

        // Configure mock to throw exception
        doThrow(new RuntimeException("Mail server error"))
                .when(emailSender).send(any(SimpleMailMessage.class));

        // Verify that the method throws RuntimeException
        assertThrows(RuntimeException.class, () -> {
            emailUtil.sendEmail(from, to, subject, body);
        });
    }
}