/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flume.sink.solr.morphline;

import com.oreilly.servlet.multipart.FilePart;
import com.oreilly.servlet.multipart.MultipartParser;
import com.oreilly.servlet.multipart.ParamPart;
import com.oreilly.servlet.multipart.Part;
import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.conf.ConfigurationException;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.source.http.HTTPSourceHandler;
import org.apache.tika.metadata.Metadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.http.HttpServletRequest;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.*;
import java.util.zip.GZIPInputStream;

/**
 * BlobHandler for HTTPSource that returns event that contains the request
 * parameters as well as the Binary Large Object (BLOB) uploaded with this
 * request.
 * <p/>
 * Note that this approach is not suitable for very large objects because it
 * buffers up the entire BLOB.
 * <p/>
 * Example client usage:
 * <p/>
 *
 * <pre>
 * curl --data-binary @sample-statuses-20120906-141433-medium.avro 'http://127.0.0.1:5140?resourceName=sample-statuses-20120906-141433-medium.avro' --header 'Content-Type:application/octet-stream' --verbose
 * </pre>
 */
public class BlobHandler implements HTTPSourceHandler {

    public static final String MAX_BLOB_LENGTH_KEY = "maxBlobLength";
    public static final int MAX_BLOB_LENGTH_DEFAULT = 100 * 1000 * 1000;
    private static final int DEFAULT_BUFFER_SIZE = 1024 * 8;
    private static final Logger LOGGER = LoggerFactory
            .getLogger(BlobHandler.class);
    public static boolean USEZIP = false;
    public static boolean USEMULTIREQ = false;
    public static boolean REMAINHEADERS = false;
    private int maxBlobLength = MAX_BLOB_LENGTH_DEFAULT;

    public BlobHandler() {
    }

    public static byte[] append(byte[] org, byte[] to) {

        byte[] newByte = new byte[org.length + to.length];

        System.arraycopy(org, 0, newByte, 0, org.length);

        System.arraycopy(to, 0, newByte, org.length, to.length);

        return newByte;

    }

    public static byte[] DeCompress(byte[] bytein) throws Exception {

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ByteArrayInputStream in = new ByteArrayInputStream(bytein);
        GZIPInputStream gunzip = new GZIPInputStream(in);
        byte[] buffer = new byte[1024];
        int n;
        try {
            while ((n = gunzip.read(buffer)) >= 0) {
                out.write(buffer, 0, n);
            }
            return out.toByteArray();
        } catch (Exception e) {
            throw e;
        } finally {
            out.close();
            in.close();
            gunzip.close();
        }
    }

    public static Map<String, String> ParseFileNameToEventHeader(String fileName) {
        HashMap<String,String> retMap=new HashMap<String,String>();
        // data format right now is:file=1|-1|1|-1
        try {
            String []header=fileName.split("|");
            //HashMap<String,String> map=new HashMap<String,String>();
            retMap.put("FP",header[0]==null?header[0]:"");
            retMap.put("PID",header[1]==null?header[1]:"");
            retMap.put("SID",header[2]==null?header[2]:"");
            retMap.put("LP",header[3]==null?header[3]:"");
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("Fomat Event Header is :" + retMap.toString());
            }
            return retMap;
        } catch (Exception e) {
            LOGGER.error(e.getMessage());
            return null;
        }
    }

    @Override
    public void configure(Context context) {
        this.maxBlobLength = context.getInteger(MAX_BLOB_LENGTH_KEY,
                MAX_BLOB_LENGTH_DEFAULT);
        USEZIP = context.getBoolean("usezip", false);
        REMAINHEADERS = context.getBoolean("remainheaders", true);
        USEMULTIREQ = context.getBoolean("usemultireq", false);
        if (this.maxBlobLength <= 0) {
            throw new ConfigurationException("Configuration parameter "
                    + MAX_BLOB_LENGTH_KEY + " must be greater than zero: "
                    + maxBlobLength);
        }
    }

    private ByteArrayOutputStream TransByteStream(InputStream in)
            throws IOException {
        int blobLength = 0, n;
        byte[] buf = new byte[Math.min(maxBlobLength, DEFAULT_BUFFER_SIZE)];
        ByteArrayOutputStream blob = null;
        while ((n = in.read(buf, 0,
                Math.min(buf.length, maxBlobLength - blobLength))) != -1) {// load
            if (blob == null) {
                blob = new ByteArrayOutputStream(n);
            }
            blob.write(buf, 0, n);
            blobLength += n;
            if (blobLength >= maxBlobLength) {
                LOGGER.warn(
                        "Request length exceeds maxBlobLength ({}), truncating BLOB event!",
                        maxBlobLength);
                break;
            }
        }
        return blob;
    }

    /*
     * by Marvin change per request to multipart httprequest use MultiPartParser
     * to convert request to multiServletRequest
     *
     * @see
     * org.apache.flume.source.http.HTTPSourceHandler#getEvents(javax.servlet
     * .http.HttpServletRequest)
     */
    @SuppressWarnings("resource")
    @Override
    public List<Event> getEvents(HttpServletRequest request) throws Exception {
        InputStream in = null;
        byte[] bytes = null;
        ByteArrayOutputStream blob = null;
        // int blobLength = 0, n;
        Event event;// lazy instance
        // byte[] buf = new byte[Math.min(maxBlobLength, DEFAULT_BUFFER_SIZE)];
        if (USEMULTIREQ) {

            MultipartParser parser = new MultipartParser(request, 2048 * 1024);// 2MB
            List<Event> resultListEvents = new ArrayList<Event>();
            Part partFilePart = null;
            while ((partFilePart = parser.readNextPart()) != null) {
                LOGGER.debug("FileName cought:" + partFilePart.getName());
                String fileName = partFilePart.getName();

                if (partFilePart.isFile()) {
                    LOGGER.debug("enter in file progress");
                    FilePart filepart = (FilePart) partFilePart;
                    in = filepart.getInputStream();
                    blob = TransByteStream(in);

                } else if (partFilePart.isParam()) {
                    LOGGER.debug("enter in parm progress");
                    ParamPart paramPart = (ParamPart) partFilePart;
                    in = new ByteArrayInputStream(paramPart.getValue());
                    blob = TransByteStream(in);
                } else {
                    // part is null
                    // do nothing
                }
                if (USEZIP) {
                    assert blob != null;
                    byte[] gzipArray = blob != null ? blob.toByteArray()
                            : new byte[0];
                    bytes = DeCompress(gzipArray);
                    // append split string in content
//                    bytes = append(bytes,
//                            new String("***************").getBytes());
                    event = EventBuilder.withBody(bytes,
                            ParseFileNameToEventHeader(fileName));
                } else {
                    LOGGER.debug("USEZIP is close");
                    event = EventBuilder.withBody(blob.toByteArray(),
                            ParseFileNameToEventHeader(fileName));
                }
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("Event File Name is:" + fileName);
                    LOGGER.debug("Event Body is:"
                            + new String(USEZIP == true ? bytes : blob
                            .toByteArray()));
                }
                resultListEvents.add(event);
            }
            return resultListEvents;
        } else {// not multi request,so parse per request
            Map<String, String> headers = getHeaders(request);// once process
            in = request.getInputStream();
            try {
                blob = TransByteStream(in);

                byte[] array = blob != null ? blob.toByteArray() : new byte[0];
                if (USEZIP) {
                    bytes = DeCompress(array);
                    long a = System.currentTimeMillis();

                    event = EventBuilder.withBody(bytes, headers);
                    LOGGER.debug("Event Body is:" + new String(bytes));
                } else {
                    event = EventBuilder.withBody(array, headers);
                    LOGGER.debug("blobEvent plug by zip: {}", event);
                }
                return Collections.singletonList(event);
            } catch (Exception e) {
                throw e;
            } finally {
                if (in != null)
                    in.close();
                if (blob != null)
                    blob.close();
            }
        }
    }

    private Map<String, String> getHeaders(HttpServletRequest request) {
        Map<String, String> requestHeaders = new HashMap<String, String>();
        if (REMAINHEADERS) {
            // Map<String, String> requestHeaders = new HashMap<String,
            // String>();
            Enumeration<?> iter = request.getHeaderNames();
            while (iter.hasMoreElements()) {
                String name = (String) iter.nextElement();
                requestHeaders.put(name, request.getHeader(name));
            }
            LOGGER.debug("requestHeaders: {}", requestHeaders);
        }
        Map<String, String> headers = new HashMap<String, String>();
        if (request.getContentType() != null) {
            headers.put(Metadata.CONTENT_TYPE, request.getContentType());
        }
        Enumeration<?> iter = request.getParameterNames();
        while (iter.hasMoreElements()) {
            String name = (String) iter.nextElement();
            headers.put(name, request.getParameter(name));
        }
        return requestHeaders;
    }

}
