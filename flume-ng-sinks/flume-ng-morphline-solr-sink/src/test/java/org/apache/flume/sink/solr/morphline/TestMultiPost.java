package org.apache.flume.sink.solr.morphline;

import org.apache.flume.source.http.HTTPSourceHandler;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by masongyu on 2015/12/24.
 */
public class TestMultiPost {
    private HTTPSourceHandler handler;

    private void SendPost(){
        try {
            handler= new BlobHandler();
            List<PostParameter> params = new ArrayList<PostParameter>();
            params.add(new PostParameter<String>("fullname", "John Doe"));
            params.add(new PostParameter<String>("headshot", "123"));
            MultipartPost post = new MultipartPost(params);
            //handler.getEvents(post);
            post.send("http://www.yourhost.com");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    public static void main(String[] args) {
        TestMultiPost test=new TestMultiPost();
        test.SendPost();
    }
}
