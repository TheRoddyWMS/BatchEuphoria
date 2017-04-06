package de.dkfz.eilslabs.batcheuphoria.execution

import de.dkfz.eilslabs.batcheuphoria.jobs.Command
import de.dkfz.eilslabs.batcheuphoria.execution.cluster.lsf.rest.RestCommand
import de.dkfz.eilslabs.batcheuphoria.execution.cluster.lsf.rest.RestResult
import de.dkfz.roddy.execution.io.ExecutionResult
import de.dkfz.roddy.tools.LoggerWrapper
import groovy.transform.NotYetImplemented
import org.apache.commons.io.IOUtils
import org.apache.http.Header
import org.apache.http.HttpEntity
import org.apache.http.auth.AuthenticationException
import org.apache.http.client.methods.CloseableHttpResponse
import org.apache.http.client.methods.HttpGet
import org.apache.http.client.methods.HttpPost
import org.apache.http.config.Registry
import org.apache.http.config.RegistryBuilder
import org.apache.http.conn.socket.ConnectionSocketFactory
import org.apache.http.conn.socket.PlainConnectionSocketFactory
import org.apache.http.conn.ssl.DefaultHostnameVerifier
import org.apache.http.conn.ssl.SSLConnectionSocketFactory
import org.apache.http.conn.ssl.TrustSelfSignedStrategy
import org.apache.http.entity.StringEntity
import org.apache.http.impl.client.CloseableHttpClient
import org.apache.http.impl.client.HttpClientBuilder
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager
import org.apache.http.message.BasicHeader
import org.apache.http.protocol.HTTP
import org.apache.http.ssl.SSLContexts
import org.apache.http.util.EntityUtils

import javax.net.ssl.SSLContext
import java.security.KeyStore

/**
 * Created by kaercher on 12.01.17.
 */
class RestExecutionService implements ExecutionService{

    private static final LoggerWrapper logger = LoggerWrapper.getLogger(RestExecutionService.class.name);

    public boolean isCertAuth = false

    private String token = ""

    private File keyStoreLocation
    private String keyStorePassword

    public final String BASE_URL

    /*REST RESOURCES*/
    public static final String RESOURCE_LOGON = "/logon"
    public static final String RESOURCE_LOGOUT = "/logout"

    RestExecutionService(String baseURL, String username, String password) throws AuthenticationException{
        this.BASE_URL = baseURL
        RestResult result = logon(username,password)
        if(result.statusCode == 200){
            this.token = new XmlSlurper().parseText(result.body).token.text()
        }else{
            logger.warning("Could not authenticate, returned HTTP status code: ${result?.statusCode}")
            throw new AuthenticationException("Could not authenticate, returned HTTP status code: ${result?.statusCode}")
        }
    }


    RestExecutionService(File keystoreLocation, String keyStorePassword){
        this.keyStoreLocation = keystoreLocation
        this.keyStorePassword = keyStorePassword

    }

    RestResult logon(String username, String password){
        String url = RESOURCE_LOGON
        String body = "<User><name>${username}</name><pass>${password}</pass></User>"
        List<Header> headers = []
        headers.add(new BasicHeader(HTTP.CONTENT_TYPE, "application/xml;charset=UTF-8"))
        headers.add(new BasicHeader("Accept", "application/xml"))
        return execute(new RestCommand(url,body,headers,RestCommand.HttpMethod.HTTPPOST))
    }


    boolean logout(){
        String url = RESOURCE_LOGOUT
        List<Header> headers = []
        headers.add(new BasicHeader(HTTP.CONTENT_TYPE, "application/xml;charset=UTF-8"))
        headers.add(new BasicHeader("Accept", "application/xml"))

        RestResult result =execute(new RestCommand(url,null,headers,RestCommand.HttpMethod.HTTPPOST))
        if(result.statusCode == 200){
            this.token = null
            return  true
        }else{
           return false
        }
    }


    RestResult execute(RestCommand restCommand) {
        String url = BASE_URL+restCommand.resource
        println("url: "+url)
        def httpClient
        if(isCertAuth)
            httpClient = createHttpClientWithClientCertAuth()
        else
            httpClient = HttpClientBuilder.create().build()

        def httpRequest

        if(restCommand.httpMethod == RestCommand.HttpMethod.HTTPPOST){
            httpRequest = new HttpPost(url)
            if(restCommand.requestBody)
                httpRequest.setEntity(new StringEntity(restCommand.requestBody,"UTF-8"))
        }else{
            httpRequest = new HttpGet(url)
        }

        if(token){
            restCommand.requestHeaders.add(new BasicHeader("Cookie", "platform_token=${token.replaceAll('"',"#quote#")}"))
        }

        if(restCommand.requestHeaders)
            httpRequest.setHeaders((Header[])restCommand.requestHeaders.toArray())

        CloseableHttpResponse response = httpClient.execute(httpRequest)

        try {
            logger.info("response status: "+response.getStatusLine())
            HttpEntity entity = response.getEntity()
            logger.info("response body: "+response.getEntity().contentType.value)
            String result = IOUtils.toString(entity.content)
            EntityUtils.consume(entity)
            return new RestResult(statusCode: response.getStatusLine().getStatusCode(),body:result,headers:response.getAllHeaders())
        } finally {
            response.close()
        }

    }


    private CloseableHttpClient createHttpClientWithClientCertAuth() {
        try {
            // read in the keystore from the filesystem, this should contain a single keypair
            KeyStore clientKeyStore = KeyStore.getInstance("PKCS12");
            clientKeyStore.load(new FileInputStream(this.keyStoreLocation), this.keyStorePassword.toCharArray());

            SSLContext sslContext = SSLContexts
                    .custom()
                    .loadKeyMaterial(clientKeyStore, this.keyStorePassword.toCharArray())
                    .loadTrustMaterial(null, new TrustSelfSignedStrategy())
                    .build();

            SSLConnectionSocketFactory sslConnectionFactory = new SSLConnectionSocketFactory(sslContext,
                    new DefaultHostnameVerifier());

            Registry<ConnectionSocketFactory> registry = RegistryBuilder.<ConnectionSocketFactory> create()
                    .register("https", sslConnectionFactory)
                    .register("http", new PlainConnectionSocketFactory())
                    .build();

            HttpClientBuilder builder = HttpClientBuilder.create();
            builder.setSSLSocketFactory(sslConnectionFactory)
            builder.setConnectionManager(new PoolingHttpClientConnectionManager(registry));

            return builder.build();
        } catch (Exception ex) {
            // log ex
            return null;
        }
    }

    @NotYetImplemented
    @Override
    ExecutionResult execute(Command command) {
        return null
    }
    @NotYetImplemented
    @Override
    ExecutionResult execute(Command command, boolean waitFor) {
        return null
    }
    @NotYetImplemented
    @Override
    ExecutionResult execute(String command) {
        return null
    }
    @NotYetImplemented
    @Override
    ExecutionResult execute(String command, boolean waitFor) {
        return null
    }
    @NotYetImplemented
    @Override
    ExecutionResult execute(String command, boolean waitForIncompatibleClassChangeError, OutputStream outputStream) {
        return null
    }
    @NotYetImplemented
    @Override
    boolean isAvailable() {
        return false
    }

    @Override
    String handleServiceBasedJobExitStatus(Command command, ExecutionResult res, OutputStream outputStream) {
        return null
    }
}