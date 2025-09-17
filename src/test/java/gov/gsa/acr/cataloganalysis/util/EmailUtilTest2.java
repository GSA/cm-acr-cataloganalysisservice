package gov.gsa.acr.cataloganalysis.util;

import lombok.extern.slf4j.Slf4j;
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
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@Slf4j
@SpringBootTest
@MockBeans({@MockBean(JavaMailSender.class)})
@ContextConfiguration(classes = {EmailUtil.class})
class EmailUtilTest2 {
    @Autowired
    private JavaMailSender emailSender;
    @Autowired
    private EmailUtil emailUtil;
    @Test
    void sendEmail() {
        // Test data
        String from = "acr-no-reply@gsa.gov";
        String to = "acrops@gsa.gov";
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
}