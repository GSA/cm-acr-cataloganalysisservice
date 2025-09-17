package gov.gsa.acr.cataloganalysis.util;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.mail.SimpleMailMessage;
import org.springframework.mail.javamail.JavaMailSender;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class EmailUtil {
    private final String PN = "EMAIL: ";
    private final JavaMailSender emailSender;

    @Value("${email.from:acr-no-reply@gsa.gov}")
    private String from;

    @Value("${email.to:acrops@gsa.gov}")
    private String to;

    public EmailUtil(JavaMailSender emailSender) {
        this.emailSender = emailSender;
    }

    /**
     * Send email to the default recipient using the default sender
     * @param subject
     * @param body
     */
    public void sendEmail(String subject, String body){
        this.sendEmail(from, to, subject, body);
    }


    /**
     * Sends an email using the configured mail server.
     *
     * @param from The email address of the sender
     * @param to The email address of the recipient
     * @param subject The subject line of the email
     * @param body The content/body of the email
     */
    public void sendEmail(String from, String to, String subject, String body) {
        try {
            SimpleMailMessage message = new SimpleMailMessage();
            message.setFrom(from);
            message.setTo(to);
            message.setSubject(subject);
            message.setText(body);

            emailSender.send(message);
            log.info("{} Email sent successfully from:{}, to: {}", PN, from, to);
        } catch (Exception e) {
            log.error("{} Failed to send email from: {}, to {}: {}", PN, from, to, e.getMessage(), e);
            throw new RuntimeException(PN+" Failed to send email", e);
        }
    }
}
