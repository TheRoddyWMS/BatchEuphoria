/*
 * Copyright (c) 2021 German Cancer Research Center (Deutsches Krebsforschungszentrum, DKFZ).
 *
 * Distributed under the MIT License (license terms are at https://www.github.com/TheRoddyWMS/Roddy/LICENSE.txt).
 */

package de.dkfz.roddy.execution

import de.dkfz.roddy.BEException
import de.dkfz.roddy.execution.io.ExecutionResult
import de.dkfz.roddy.execution.jobs.Command
import de.dkfz.roddy.execution.jobs.cluster.lsf.rest.RestSubmissionCommand
import de.dkfz.roddy.execution.jobs.cluster.lsf.rest.RestResult
import groovy.transform.CompileStatic
import org.apache.commons.io.IOUtils
import org.apache.commons.text.StringEscapeUtils
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
import org.apache.http.impl.client.HttpClients
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager
import org.apache.http.message.BasicHeader
import org.apache.http.protocol.HTTP
import org.apache.http.ssl.SSLContextBuilder
import org.apache.http.ssl.SSLContexts
import org.apache.http.ssl.TrustStrategy
import org.apache.http.util.EntityUtils
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import sun.reflect.generics.reflectiveObjects.NotImplementedException

import javax.net.ssl.HostnameVerifier
import javax.net.ssl.SSLContext
import javax.net.ssl.SSLSession
import java.security.KeyStore
import java.security.cert.CertificateException
import java.security.cert.X509Certificate
import java.time.Duration
import java.time.LocalDateTime

/**
 * Execution service for cluster systems with REST services. It is currently only used for LSF.
 */
@CompileStatic
class RestExecutionService implements BEExecutionService {
    final Logger log = LoggerFactory.getLogger(RestExecutionService.class)

    public boolean isCertAuth = false // true if certificate authentication is used

    private String token = ""
    private LocalDateTime tokenDate = LocalDateTime.now()
    private String username
    private String password

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
        this.username = username
        this.password = password

        if (isAvailable())
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
        String body = "<User><name>${StringEscapeUtils.escapeXml10(username)}</name><pass>${StringEscapeUtils.escapeXml10(password)}</pass></User>"
        List<Header> headers = []
        headers.add(new BasicHeader(HTTP.CONTENT_TYPE, "application/xml;charset=UTF-8"))
        headers.add(new BasicHeader("Accept", "application/xml"))
        RestResult result = execute(new RestSubmissionCommand(RESOURCE_LOGON, body, headers, RestSubmissionCommand.HttpMethod.HTTPPOST))
        log.debug("response status: {} ", result.statusCode)
        if (result.statusCode != 200)
            throw new AuthenticationException("Could not authenticate, returned HTTP status code: ${result.statusCode}")

        this.token = new XmlSlurper().parseText(result.body).getProperty("token").toString()
        return result
    }


    boolean logout() {
        List<Header> headers = []
        headers.add(new BasicHeader(HTTP.CONTENT_TYPE, "application/xml;charset=UTF-8"))
        headers.add(new BasicHeader("Accept", "application/xml"))

        RestResult result = execute(new RestSubmissionCommand(RESOURCE_LOGOUT, null, headers, RestSubmissionCommand.HttpMethod.HTTPPOST))
        if (result.statusCode != 200)
            throw new AuthenticationException("Could not log out, returned HTTP status code: ${result.statusCode}")

        this.token = null
        return true

    }

    /** Note: This implemented method is not a proper implementation of
     *  `ExecutionResult execute(Command, Duration)` because `RestSubmissionCommand` is a *sub*type
     *  of `Command`, which means this method is not valid anymore for valid arguments of the
     *  superclass, thus violating the Liskov-Substitution Principle. The method parameters are
     *  contra-variant, so a *super*type of Command would be a valid parameter type.
     */
    @Override
    ExecutionResult execute(Command command, Duration timeout) {
        throw new IllegalArgumentException("Not a RestSubmissionCommand: ${command.class.canonicalName}")
    }
    RestResult execute(RestSubmissionCommand restCommand, Duration timeout) {
        execute(restCommand, true, timeout)
    }

    /**
     * Here is the same problem with the LSP and contravariance.
     */
    @Override
    ExecutionResult execute(Command command, boolean waitFor, Duration timeout) {
        throw new IllegalArgumentException("Not a RestSubmissionCommand: ${command.class.canonicalName}")
    }
    RestResult execute(RestSubmissionCommand restCommand, boolean waitFor = true,
                       Duration timeout = Duration.ZERO) {
        if (!waitFor) {
            throw new IllegalArgumentException(
                    "RestExecutionService.execute does not implement asynchronous execution with waitFor = false")
        }
        String url = BASE_URL + restCommand.resource
        log.debug("request url: {}", url)
        CloseableHttpClient httpClient
        if (isCertAuth) {
            httpClient = createHttpClientWithClientCertAuth()
        } else {
            httpClient = createHttpClientWithCredentialAuth()
        }
        def httpRequest

        if (restCommand.httpMethod == RestSubmissionCommand.HttpMethod.HTTPPOST) {
            httpRequest = new HttpPost(url)
            if (restCommand.requestBody)
                log.debug("request body: {}", restCommand.requestBody)
                httpRequest.setEntity(new StringEntity(restCommand.requestBody, "UTF-8"))
        } else {
            httpRequest = new HttpGet(url)
        }

        if (token) {
            restCommand.requestHeaders.add(new BasicHeader("Cookie",
                    "platform_token=${token.replaceAll('"', "#quote#")}"))
        }

        if (restCommand.requestHeaders) {
            log.debug("request headers: {}", restCommand.requestHeaders)
            httpRequest.setHeaders(restCommand.requestHeaders.toArray(new Header[0]) as Header)
        }


        CloseableHttpResponse response = httpClient.execute(httpRequest)

        try {
            log.debug("response status: {}", response.getStatusLine())
            HttpEntity entity = response.getEntity()
            String result = IOUtils.toString(entity.content)
            log.debug("response body: {}", result)
            EntityUtils.consume(entity)

            if (response.getStatusLine().statusCode == 403 && !isCertAuth && Duration.between(tokenDate, LocalDateTime.now()).seconds > 60) {
                if (this.logon(username, password)) {
                    return this.execute(restCommand)
                }
            }
            this.tokenDate = LocalDateTime.now()
            return new RestResult(restCommand, response.getAllHeaders(), result, response.getStatusLine().getStatusCode())
        } finally {
            response.close()
        }
    }

    private CloseableHttpClient createHttpClientWithCredentialAuth() {
        //TODO Currently all certificates are trusted for testing. It has to be fixed.
        SSLContextBuilder builder = new SSLContextBuilder();
        builder.loadTrustMaterial(null, new TrustStrategy() {
            @Override
            public boolean isTrusted(X509Certificate[] chain, String authType)
                    throws CertificateException {
                return true;
            }
        });

        HostnameVerifier hostnameVerifierAllowAll = new HostnameVerifier() {
            @Override
            public boolean verify(String hostname, SSLSession session) {
                return true;
            }
        };

        SSLConnectionSocketFactory sslSocketFactory = new SSLConnectionSocketFactory(builder.build(), hostnameVerifierAllowAll);

        return HttpClients.custom().setSSLSocketFactory(
                sslSocketFactory).build();

    }


    private CloseableHttpClient createHttpClientWithClientCertAuth() {
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

    }


    @Override
    boolean isAvailable() {
        List<Header> headers = []
        headers.add(new BasicHeader(HTTP.CONTENT_TYPE, "application/xml;charset=UTF-8"))
        headers.add(new BasicHeader("Accept", "application/xml"))

        RestResult result = execute(new RestSubmissionCommand(
                RESOURCE_PING, null,
                headers, RestSubmissionCommand.HttpMethod.HTTPPOST))
        if (result.statusCode != 200)
            throw new BEException("Web service is not available, returned HTTP status code: ${result.statusCode}")

        return true
    }

    /** TODO Refactor (violated LSP and parallel inheritance hierarchy smell)
     *  The existence of this method with just an exception is an indication that there is a faulty design, because
     *  the subclass (RestExecutionService) cannot handle all arguments that are valid for the superclass method,
     *  which is a violation of the Liskov-Substitution-Principle. This may be related to the bad "parallel inheritance
     *  hierarchy" smell.
     */
    @Override
    ExecutionResult execute(Command command, boolean waitFor = null) {
        throw new NotImplementedException()
    }

    @Deprecated
    @Override
    ExecutionResult execute(String command, Duration timeout) {
        throw new NotImplementedException()
    }

    @Deprecated
    @Override
    ExecutionResult execute(String command, boolean waitFor, Duration timeout) {
        throw new NotImplementedException()
    }


    @Deprecated
    @Override
    ExecutionResult execute(String command,
                            boolean waitForIncompatibleClassChangeError = null,
                            OutputStream outputStream = null) {
        throw new NotImplementedException()
    }


    @Override
    File queryWorkingDirectory() {
        throw new NotImplementedException()
    }

}
