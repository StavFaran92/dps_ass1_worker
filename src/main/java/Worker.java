import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.InstanceProfileCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.CannedAccessControlList;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.MessageAttributeValue;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import com.amazonaws.util.EC2MetadataUtils;
import org.apache.commons.io.FileUtils;
import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.util.PDFImageWriter;
import org.apache.pdfbox.util.PDFText2HTML;
import org.apache.pdfbox.util.PDFTextStripper;

import java.awt.image.BufferedImage;
import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

//gets message SQS from manager
//recieveMessage(sqs, queueUrl);
//downloads the PDF
//performs the requested operation
//uploads the resulting work to S3
//send SQS indicating the job params

class MessageInfo{
    String action;
    String file_url;
    String task_id;

    public MessageInfo(String action, String file_url, String task_id) {
        this.action = action;
        this.file_url = file_url;
        this.task_id = task_id;
    }

    public String getAction() {
        return action;
    }

    public String getFile_url() {
        return file_url;
    }

    public String getTask_id() {
        return task_id;
    }
}

public class Worker {

    //MACROS//
    private static String ACTION = "action";
    private static String FILE_URL = "file_url";
    private static String TASK_ID = "task_id";

    //VARIABLES//
    private static AmazonS3Client s3;

    private static AmazonSQS sqs;

    private static String bucketName;

    private static String inputQueueUrl;
    private static String outputQueueUrl;

    public static void main(String[]args) {

        inputQueueUrl = args[0];
        outputQueueUrl = args[1];
        bucketName = args[2];

        initAwsServices();

        execute();
    }

    private static void initAwsServices(){

        sqs = AmazonSQSClientBuilder.standard()
                .withCredentials(new InstanceProfileCredentialsProvider(false))
                .withRegion("us-east-1")
                .build();

        s3 = (AmazonS3Client) AmazonS3ClientBuilder.standard()
                .withCredentials(new InstanceProfileCredentialsProvider(false))
                .withRegion("us-east-1")
                .build();
    }

    private static void execute() {

        while(true) {

            final Message message = receiveMessage();

            if( message != null ) {

                final MessageInfo messageInfo = processMessage(message);

                try {

                    performTask(messageInfo);

                } catch (IOException e) {

                    handleException(messageInfo, e);
                }
                finally {
                    sqs.deleteMessage( inputQueueUrl, message.getReceiptHandle());
                }
            }
            else{
                System.out.println("Finished work, shutting down.\n");

                break;
            }
        }
    }

    private static Message receiveMessage(){

        System.out.println("Receiving messages from MyQueue.\n");

        ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(inputQueueUrl)
                .withMaxNumberOfMessages(1)
                .withMessageAttributeNames(ACTION, FILE_URL, TASK_ID)
                .withAttributeNames("");
        List<Message> messages = sqs.receiveMessage(receiveMessageRequest).getMessages();

        return messages.isEmpty() ? null : messages.get(0);
    }

    private static void performTask(final MessageInfo messageInfo) throws IOException {

        System.out.println("Performing task.\n");

        String action = messageInfo.getAction();
        String inputUrl = messageInfo.getFile_url();
        String fileName = action + "-" + inputUrl.substring(inputUrl.lastIndexOf("/") + 1);

        PDDocument doc = getDocFromUrl(inputUrl);

        File file = applyActionOnDoc( action, doc );

        String outputUrl = "";
        if(file != null) {

            outputUrl = uploadFileToS3(file, fileName);

            String message;
            if (!outputUrl.equals("")) {

                message = prepareMessage(action, inputUrl, outputUrl);

                sendSQSMessage(messageInfo, message);
            }
        }
    }


    private static MessageInfo processMessage(Message message) {

        System.out.println("Process Message.\n");

        Map<String, MessageAttributeValue> msgAttributes = message.getMessageAttributes();

        return new MessageInfo(
                msgAttributes.get(ACTION).getStringValue(),
                msgAttributes.get(FILE_URL).getStringValue(),
                msgAttributes.get(TASK_ID).getStringValue());
    }

    private static PDDocument getDocFromUrl(String url) throws IOException {

        System.out.println("Get Document from url.\n");

        return PDDocument.load(new URL(url));
    }

    private static File applyActionOnDoc(String action, PDDocument doc) throws IOException {

        System.out.println("Apply action on document.\n");

        if ("ToImage".equals(action)) {
            return convertPDFToImage(doc);
        } else if ("ToHTML".equals(action)) {
            return  convertPDFToHTML(doc);
        } else if ("ToText".equals(action)) {
            return convertPDFToText(doc);
        } else {
            return null;
        }
    }

    private static File convertPDFToImage(PDDocument doc) throws IOException {

        PDFImageWriter imageWriter = new PDFImageWriter();
        boolean success = imageWriter.writeImage(doc, "jpg","",
                1,1,"img", BufferedImage.TYPE_INT_RGB,256);
        doc.close();
        if (!success) {
            System.err.println("Error: no writer found for image format '"
                    + "jpg" + "'");
        }
        else{
            return new File("img1.jpg");
        }
        return null;

    }

    private static File convertPDFToText(PDDocument doc) throws IOException {

        String text = new PDFTextStripper().getText(doc);
        final File file = new File("file_name.txt");
        FileUtils.writeStringToFile(file, text, "ISO-8859-1");
        doc.close();

        return file;
    }

    private static File convertPDFToHTML(PDDocument doc) throws IOException {

        PDFText2HTML stripper = new PDFText2HTML("UTF-8");
        String text = stripper.getText(doc);
        final File file = new File("file_name.txt");
        FileUtils.writeStringToFile(file, text, "ISO-8859-1");
        doc.close();

        return file;

    }

    private static String uploadFileToS3(File file, String fileName){

        System.out.println("Upload file to s3.\n");

        String fileResourceLocation = "output/" + UUID.randomUUID() + fileName;

        s3.putObject(new PutObjectRequest(bucketName, fileResourceLocation, file)
                .withCannedAcl(CannedAccessControlList.PublicRead));

        return s3.getResourceUrl(bucketName, fileResourceLocation);
    }

    private static String prepareMessage(String action, String inputUrl, String outputUrl){
        System.out.println("prepare message for send.\n");

        return action + ":" + inputUrl + "\t" + outputUrl + "\n";
    }

    private static void handleException(final MessageInfo messageInfo, Exception e){

        System.out.println("Exception in Worker, Rebooting.\n");

        String message = prepareMessage(messageInfo.getAction(), messageInfo.getFile_url(), e.toString());

        sendSQSMessage(messageInfo, message);

    }

    private static void sendSQSMessage(final MessageInfo messageInfo, String message){

        sqs.sendMessage(new SendMessageRequest(outputQueueUrl, message)
                .withMessageAttributes(new HashMap<String, MessageAttributeValue>(){{
                    put(TASK_ID, new MessageAttributeValue().withDataType("String").withStringValue(messageInfo.getTask_id()));
                }}));
    }
}


