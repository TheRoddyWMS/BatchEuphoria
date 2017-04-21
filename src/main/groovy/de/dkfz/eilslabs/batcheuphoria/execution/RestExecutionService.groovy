/*
 * Copyright (c) 2017 eilslabs.
 *
 * Distributed under the MIT License (license terms are at https://www.github.com/eilslabs/Roddy/LICENSE.txt).
 */

package de.dkfz.eilslabs.batcheuphoria.execution

import de.dkfz.eilslabs.batcheuphoria.jobs.Command
import de.dkfz.eilslabs.batcheuphoria.execution.cluster.lsf.rest.RestCommand
import de.dkfz.eilslabs.batcheuphoria.execution.cluster.lsf.rest.RestResult
import de.dkfz.roddy.execution.io.ExecutionResult
import de.dkfz.roddy.tools.LoggerWrapper
import groovy.transform.CompileStatic
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
 * Execution service for cluster systems with REST services. It is currently only used for LSF
 * Created by kaercher on 12.01.17.
 */
@CompileStatic
class RestExecutionService implements ExecutionService {

    private static final LoggerWrapper logger = LoggerWrapper.getLogger(RestExecutionService.class.name);

    public boolean isCertAuth = false //true if certificate authentication is used

    private String token = ""

    private File keyStoreLocation
    private String keyStorePassword

    public final String BASE_URL

    /*REST RESOURCES*/
    public static final String RESOURCE_LOGON = "/logon"
    public static final String RESOURCE_LOGOUT = "/logout"
    public static final String RESOURCE_PING = "/ping"

    /**
     * Authenticate with username/password
     * @param baseURL - web service url e.g. https://localhost:8080/platform/ws
     * @param username
     * @param password
     * @throws AuthenticationException
     */
    RestExecutionService(String baseURL, String username, String password) throws AuthenticationException {
        this.BASE_URL = baseURL
        logon(username, password)
    }

    /**
     * Authenticate with X.509 certificate
     * @param keystoreLocation
     * @param keyStorePassword
     */
    RestExecutionService(File keystoreLocation, String keyStorePassword) {
        this.keyStoreLocation = keystoreLocation
        this.keyStorePassword = keyStorePassword
        isCertAuth = true
    }

    RestResult logon(String username, String password) {
        String body = "<User><name>${username}</name><pass>${password}</pass></User>"
        List<Header> headers = []
        headers.add(new BasicHeader(HTTP.CONTENT_TYPE, "application/xml;charset=UTF-8"))
        headers.add(new BasicHeader("Accept", "application/xml"))
        RestResult result = execute(new RestCommand(RESOURCE_LOGON, body, headers, RestCommand.HttpMethod.HTTPPOST))
        if (result.statusCode != 200)
            throw new AuthenticationException("Could not authenticate, returned HTTP status code: ${result.statusCode}")

        this.token = new XmlSlurper().parseText(result.body).getProperty("token").toString()
        return result
    }


    boolean logout() {
        List<Header> headers = []
        headers.add(new BasicHeader(HTTP.CONTENT_TYPE, "application/xml;charset=UTF-8"))
        headers.add(new BasicHeader("Accept", "application/xml"))

        RestResult result = execute(new RestCommand(RESOURCE_LOGOUT, null, headers, RestCommand.HttpMethod.HTTPPOST))
        if (result.statusCode != 200)
            throw new AuthenticationException("Could not log out, returned HTTP status code: ${result.statusCode}")

        this.token = null
        return true

    }


    RestResult execute(RestCommand restCommand) {
        String url = BASE_URL + restCommand.resource
        CloseableHttpClient httpClient
        if (isCertAuth)
            httpClient = createHttpClientWithClientCertAuth()
        else
            httpClient = HttpClientBuilder.create().build()

        def httpRequest

        if (restCommand.httpMethod == RestCommand.HttpMethod.HTTPPOST) {
            httpRequest = new HttpPost(url)
            if (restCommand.requestBody)
                httpRequest.setEntity(new StringEntity(restCommand.requestBody, "UTF-8"))
        } else {
            httpRequest = new HttpGet(url)
        }

        if (token) {
            restCommand.requestHeaders.add(new BasicHeader("Cookie", "platform_token=${token.replaceAll('"', "#quote#")}"))
        }

        if (restCommand.requestHeaders)
            httpRequest.setHeaders((Header[]) restCommand.requestHeaders.toArray())

        CloseableHttpResponse response = httpClient.execute(httpRequest)

        try {
            logger.info("response status: " + response.getStatusLine())
            HttpEntity entity = response.getEntity()
            logger.info("response body: " + response.getEntity().contentType.value)
            String result = IOUtils.toString(entity.content)
            EntityUtils.consume(entity)
            return new RestResult(response.getAllHeaders(), result, response.getStatusLine().getStatusCode())
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
    ExecutionResult execute(Command command, boolean waitFor = false) {
        return null
    }

    @NotYetImplemented
    @Override
    ExecutionResult execute(String command, boolean waitForIncompatibleClassChangeError = false, OutputStream outputStream = null) {
        return null
    }

    @Override
    boolean isAvailable() {
        List<Header> headers = []
        headers.add(new BasicHeader(HTTP.CONTENT_TYPE, "application/xml;charset=UTF-8"))
        headers.add(new BasicHeader("Accept", "application/xml"))

        RestResult result = execute(new RestCommand(RESOURCE_PING, null, headers, RestCommand.HttpMethod.HTTPPOST))
        if (result.statusCode != 200)
            throw new AuthenticationException("Web service is not available, returned HTTP status code: ${result.statusCode}")

        return true
    }

    @NotYetImplemented
    @Override
    String handleServiceBasedJobExitStatus(Command command, ExecutionResult res, OutputStream outputStream) {
        return null
    }
}