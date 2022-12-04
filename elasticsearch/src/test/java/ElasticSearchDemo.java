import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequestBuilder;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.MultiGetItemResponse;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.MultiMatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.junit.Test;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

/**
 * @program: elasticsearch
 * @description:
 * @author: gyg
 * @create: 2021-01-20 21:57
 **/
public class ElasticSearchDemo {

    @Test
    public void getTest(){

        // 指定yml文件的集群名字
        Settings build = Settings.builder().put("cluster.name", "my-application").build();
        try {
            // 获取
            TransportClient client = new PreBuiltTransportClient(build)
                    // 单机  为什么不是9200 呢(9300是tcp通讯端口，集群间和TCPClient都走的它，9200是http协议的RESTful接口)
                    .addTransportAddress(new TransportAddress(InetAddress.getByName("192.168.238.200"),9300));
                    // 集群
            // .addTransportAddresses(new TransportAddress(InetAddress.getByName("192.168.238.200"),9200),new TransportAddress(InetAddress.getByName("192.168.238.201"),9200));
            GetResponse documentFields = client.prepareGet("lib", "user", "1").execute().actionGet();
            System.out.println(documentFields.getSourceAsString());
            Map<String, Object> sourceAsMap = documentFields.getSourceAsMap();
            System.out.println(sourceAsMap);

        } catch (UnknownHostException e) {
            e.printStackTrace();
        }

    }

    @Test
    public void putDocumentTest() throws IOException {
        // 指定yml文件的集群名字
        Settings build = Settings.builder().put("cluster.name", "my-application").build();
        // 获取
        TransportClient client = new PreBuiltTransportClient(build)
                // 单机  为什么不是9200 呢(9300是tcp通讯端口，集群间和TCPClient都走的它，9200是http协议的RESTful接口)
                .addTransportAddress(new TransportAddress(InetAddress.getByName("192.168.238.200"),9300));
        // 集群
        // .addTransportAddresses(new TransportAddress(InetAddress.getByName("192.168.238.200"),9200),new TransportAddress(InetAddress.getByName("192.168.238.201"),9200));

        XContentBuilder doc = XContentFactory.jsonBuilder()
                .startObject()
                .field("id", 1)
                .field("title", "郭一光")
                .field("content", "中华人民共和国是个伟大的国家，我为我的国家和民族自豪和骄傲")
                .field("postdate", "2021-01-20")
                .field("url", "www.baidu.com")
                .endObject();

        IndexResponse indexResponse = client.prepareIndex("index1", "blog", "10").setSource(doc).get();
        // CREATED
        System.out.println(indexResponse.status());

        // 因为对title和content 进行了 中文分词，查看能否拿到数据   ???? 还有拼音分词(拼音插件)
        // match 查询
        MatchQueryBuilder query = QueryBuilders.matchQuery("title", "光郭");
        SearchResponse response = client.prepareSearch("index1")
                .setQuery(query)
                .get();
        SearchHits hits = response.getHits();
        SearchHit[] hits1 = hits.getHits();
        for (SearchHit h:hits1) {
//            {"id":1,"title":"郭一光","content":"中华人民共和国是个伟大的国家，我为我的国家和民族自豪和骄傲","postdate":"2021-01-20","url":"www.baidu.com"}
//            {"id":22,"title":"郭一光22","content":"中华人民共和国是个伟大的国家，我为我的国家和民族自豪和骄傲","postdate":"2021-01-20","url":"www.baidu.com"}
//            {"id":22,"title":"郭一光33","content":"中华人民共和国是个伟大的国家，我为我的国家和民族自豪和骄傲","postdate":"2021-01-20","url":"www.baidu.com"}
            System.out.println(h.getSourceAsString());
            Map<String, Object> sourceAsMap = h.getSourceAsMap();
            for (Map.Entry<String,Object> so:sourceAsMap.entrySet()) {
            }
        }



    }


    @Test
    public void deleteDocumentTest() throws IOException {
        // 指定yml文件的集群名字
        Settings build = Settings.builder().put("cluster.name", "my-application").build();
        // 获取
        TransportClient client = new PreBuiltTransportClient(build)
                // 单机  为什么不是9200 呢(9300是tcp通讯端口，集群间和TCPClient都走的它，9200是http协议的RESTful接口)
                .addTransportAddress(new TransportAddress(InetAddress.getByName("192.168.238.200"),9300));
        // 集群
        // .addTransportAddresses(new TransportAddress(InetAddress.getByName("192.168.238.200"),9200),new TransportAddress(InetAddress.getByName("192.168.238.201"),9200));
        DeleteResponse deleteResponse = client.prepareDelete("index1", "blog", "10").get();
        //  OK
        System.out.println(deleteResponse.status());
    }

    @Test
    public void updateDocumentTest() throws IOException, ExecutionException, InterruptedException {
        // 指定yml文件的集群名字
        Settings build = Settings.builder().put("cluster.name", "my-application").build();
        // 获取
        TransportClient client = new PreBuiltTransportClient(build)
                // 单机  为什么不是9200 呢(9300是tcp通讯端口，集群间和TCPClient都走的它，9200是http协议的RESTful接口)
                .addTransportAddress(new TransportAddress(InetAddress.getByName("192.168.238.200"),9300));
        // 集群
        // .addTransportAddresses(new TransportAddress(InetAddress.getByName("192.168.238.200"),9200),new TransportAddress(InetAddress.getByName("192.168.238.201"),9200));

        UpdateRequest updateRequest = new UpdateRequest();
        UpdateRequest doc = updateRequest.index("index1")
                .type("blog")
                .id("10")
                .doc(
                        XContentFactory.jsonBuilder()
                                .startObject()
                                .field("title", "慕容皝修改2")
                                .endObject()
                );
        UpdateResponse updateResponse = client.update(doc).get();
        // OK
        System.out.println(updateResponse.status());
    }


    /**
    * @Description: 批量查询
    */
    @Test
    public void mgetTest() throws IOException, ExecutionException, InterruptedException {
        // 指定yml文件的集群名字
        Settings build = Settings.builder().put("cluster.name", "my-application").build();
        // 获取
        TransportClient client = new PreBuiltTransportClient(build)
                // 单机  为什么不是9200 呢(9300是tcp通讯端口，集群间和TCPClient都走的它，9200是http协议的RESTful接口)
                .addTransportAddress(new TransportAddress(InetAddress.getByName("192.168.238.200"),9300));
        // 集群
        // .addTransportAddresses(new TransportAddress(InetAddress.getByName("192.168.238.200"),9200),new TransportAddress(InetAddress.getByName("192.168.238.201"),9200));
        MultiGetResponse multiGetItemResponses = client.prepareMultiGet()
                // 可以指定多个库，多个id
                .add("index1", "blog", "10","1")
                .add("lib", "user", "1")
                .get();
        MultiGetItemResponse[] responses = multiGetItemResponses.getResponses();
        for (MultiGetItemResponse re :responses ) {
            if (null != re.getResponse()) {
                // {"id":1,"title":"慕容皝修改2","content":"中华人民共和国是个伟大的国家，我为我的国家和民族自豪和骄傲","postdate":"2021-01-20","url":"www.baidu.com"}
                // {"first_name":"john","last_name":"Smith","age":40,"about":"I like  ......","interests":["music","cookie"]}
                System.out.println(re.getResponse().getSourceAsString());
            }
        }
    }

    /**
     * @Description: 批量操作（演示批量添加）
     */
    @Test
    public void bulkTest() throws IOException, ExecutionException, InterruptedException {
        // 指定yml文件的集群名字
        Settings build = Settings.builder().put("cluster.name", "my-application").build();
        // 获取
        TransportClient client = new PreBuiltTransportClient(build)
                // 单机  为什么不是9200 呢(9300是tcp通讯端口，集群间和TCPClient都走的它，9200是http协议的RESTful接口)
                .addTransportAddress(new TransportAddress(InetAddress.getByName("192.168.238.200"),9300));
        // 集群
        // .addTransportAddresses(new TransportAddress(InetAddress.getByName("192.168.238.200"),9200),new TransportAddress(InetAddress.getByName("192.168.238.201"),9200));

        BulkRequestBuilder bulkRequestBuilder = client.prepareBulk();
        bulkRequestBuilder.add(client.prepareIndex("index1","blog","22").setSource(
                XContentFactory.jsonBuilder()
                        .startObject()
                        .field("id", 22)
                        .field("title", "郭一光22")
                        .field("content", "中华人民共和国是个伟大的国家，我为我的国家和民族自豪和骄傲")
                        .field("postdate", "2021-01-20")
                        .field("url", "www.baidu.com")
                        .endObject()
        ));
        bulkRequestBuilder.add(client.prepareIndex("index1","blog","33").setSource(
                XContentFactory.jsonBuilder()
                        .startObject()
                        .field("id", 22)
                        .field("title", "郭一光33")
                        .field("content", "中华人民共和国是个伟大的国家，我为我的国家和民族自豪和骄傲")
                        .field("postdate", "2021-01-20")
                        .field("url", "www.baidu.com")
                        .endObject()
        ));

        BulkResponse bulkItemResponses = bulkRequestBuilder.get();
        if (bulkItemResponses.hasFailures()) {
            System.out.println("失败了");
        }
    }


    /**
     * @Description: 查询全部
     */
    @Test
    public void matchAllTest() throws IOException, ExecutionException, InterruptedException {
        Settings build = Settings.builder().put("cluster.name", "my-application").build();
        // 获取
        TransportClient client = new PreBuiltTransportClient(build)
                // 单机  为什么不是9200 呢(9300是tcp通讯端口，集群间和TCPClient都走的它，9200是http协议的RESTful接口)
                .addTransportAddress(new TransportAddress(InetAddress.getByName("192.168.238.200"),9300));
        // 集群
        // .addTransportAddresses(new TransportAddress(InetAddress.getByName("192.168.238.200"),9200),new TransportAddress(InetAddress.getByName("192.168.238.201"),9200));
        QueryBuilder qb = QueryBuilders.matchAllQuery();
        SearchResponse response = client.prepareSearch("index1")
                .setQuery(qb)
                // 默认显示 10条
                .setSize(3)
                .get();
        SearchHits hits = response.getHits();
        for (SearchHit sh :hits.getHits()) {
            // sh.getSourceAsMap()
//            {"id":1,"title":"慕容皝修改2","content":"中华人民共和国是个伟大的国家，我为我的国家和民族自豪和骄傲","postdate":"2021-01-20","url":"www.baidu.com"}
//            {"id":22,"title":"郭一光22","content":"中华人民共和国是个伟大的国家，我为我的国家和民族自豪和骄傲","postdate":"2021-01-20","url":"www.baidu.com"}
//            {"id":22,"title":"郭一光33","content":"中华人民共和国是个伟大的国家，我为我的国家和民族自豪和骄傲","postdate":"2021-01-20","url":"www.baidu.com"}
            System.out.println(sh.getSourceAsString());
        }
    }

    @Test
    public void mytest()  {
        // 反转链表 input 1 2 3 4 5 null
        // output  null 5 4 3 2 1


    }
    }




}
    
    