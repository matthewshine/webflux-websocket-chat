package sample.webflux.websocket.netty.handler;

import java.io.IOException;
import java.util.*;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.reactive.socket.WebSocketHandler;
import org.springframework.web.reactive.socket.WebSocketSession;

import reactor.core.publisher.DirectProcessor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

public class ServerWebSocketHandler implements WebSocketHandler {
    private final DirectProcessor<WebSocketSessionHandler> connectedProcessor;
    private final List<WebSocketSessionHandler> sessionhanlderList;
    private final Map<String, WebSocketSessionHandler> clients = Collections.synchronizedMap(new LinkedHashMap<>());
    private final Logger logger = LoggerFactory.getLogger(getClass());
    public ServerWebSocketHandler() {
        connectedProcessor = DirectProcessor.create();
        sessionhanlderList = new LinkedList<WebSocketSessionHandler>();
    }

    @Override
    public Mono<Void> handle(WebSocketSession session) {
        WebSocketSessionHandler sessionHandler = new WebSocketSessionHandler();
        System.out.println("建立websocket连接时，订阅connected和disconnected两个sink，消息收发通过session,不会二次调用");
        //在sessionHandler添加了建立和断开连接的processor,这里订阅这两个hot stream,后续事件触发时会执行subcribe的code
        //获取connected对象processor,添加到sessionhanlderList
        sessionHandler
                .connected() //当sessionhandler中的connected触发时，执行下面方法，设定当前sessionhandler到全局变量
                .subscribe(value -> {
                            sessionhanlderList.add(sessionHandler);
                            clients.put(sessionHandler.getClientId(),sessionHandler);
                            logger.info("user {} connected,there are currently {} people online",sessionHandler.getClientId(),clients.size());
                    try {
                        //messageType 1代表上线 2代表下线 3代表在线名单 4代表普通消息
                        //先给所有人发送通知，说我上线了
                        Map<String,Object> map1 = new HashMap();
                        map1.put("messageType",1);
                        map1.put("username",sessionHandler.getClientId());
                        sendMessageAll(JSON.toJSONString(map1),sessionHandler.getClientId());


                        //给自己发一条消息：告诉自己现在都有谁在线
                        Map<String,Object> map2 =  new HashMap();
                        map2.put("messageType",3);
                        //移除掉自己
                        Set<String> set = clients.keySet();
                        map2.put("onlineUsers",set);
                        sendMessageTo(JSON.toJSONString(map2),sessionHandler.getClientId());
                    }
                    catch (IOException e){
                        logger.info(sessionHandler.getClientId()+"上线的时候通知所有人发生了错误");
                    }
                        }

                );

        //获取connected对象processor,从sessionhanlderList移除closed session
        sessionHandler
                .disconnected()
                .subscribe(value -> {
                    sessionhanlderList.remove(sessionHandler);
                    clients.remove(sessionHandler.getClientId());
                    logger.info("user {} disconnected,there are currently {} people online",sessionHandler.getClientId(),clients.size());
                    try {
                        //messageType 1代表上线 2代表下线 3代表在线名单  4代表普通消息
                        Map<String,Object> map1 = new HashMap();
                        map1.put("messageType",2);
                        map1.put("onlineUsers",clients.keySet());
                        map1.put("username",sessionHandler.getClientId());
                        sendMessageAll(JSON.toJSONString(map1),sessionHandler.getClientId());
                    }
                    catch (IOException e){
                        logger.info(sessionHandler.getClientId()+"下线的时候通知所有人发生了错误");
                    }
                    }
                );

		Flux<String> receiveAll =
			sessionHandler
				.receive()		 //订阅receiveProcessor的通道
				.subscribeOn(Schedulers.elastic())
				.doOnNext(message -> logger.info("Server Received: [{}]", message))
                .takeUntil(value -> !sessionHandler.isConnected())
//                .doOnNext(message -> sessionHandler.send(message))
                .doOnNext(message -> handleMessage(sessionHandler,message))
                .doOnNext(message -> logger.info("Server Sent: [{}]", message));

		receiveAll.subscribe();

        //定义发送sessionHandler的条件时，建立websocketsession连接时
        connectedProcessor.sink().next(sessionHandler);

        //调用sessionHandler的handle方法处理session的内容
        return sessionHandler.handle(session);
    }


    //定义一个方法，获取sessionHandler的处理器
    public Flux<WebSocketSessionHandler> connected() {
        return connectedProcessor;
    }

    public void handleMessage(WebSocketSessionHandler sessionHandler,String message){

        try {
            logger.info("来自客户端消息：" + message+"客户端的id是："+sessionHandler.getClientId());
            JSONObject jsonObject = JSON.parseObject(message);
            String textMessage = jsonObject.getString("message");
            String fromusername = jsonObject.getString("username");
            String tousername = jsonObject.getString("to");
            //如果不是发给所有，那么就发给某一个人
            //messageType 1代表上线 2代表下线 3代表在线名单  4代表普通消息
            Map<String,Object> map1 = new HashMap();
            map1.put("messageType",4);
            map1.put("textMessage",textMessage);
            map1.put("fromusername",fromusername);
            if(tousername.equals("All")){
                map1.put("tousername","所有人");
                sendMessageAll(JSON.toJSONString(map1),fromusername);
            }
            else{
                map1.put("tousername",tousername);
                sendMessageTo(JSON.toJSONString(map1),tousername);
            }
        }
        catch (Exception e){
            logger.info("发生了错误了");
        }

    }

    public void sendMessageTo(String message, String toUserName) throws IOException {

        WebSocketSessionHandler webSocketSessionHandler = clients.get(toUserName);
        if(null!=webSocketSessionHandler){
            webSocketSessionHandler
                    .send(message);
        }else {
            logger.info("消息发送失败，找不到session");
        }


    }

    public void sendMessageAll(String message, String toUserName) throws IOException {
        Flux.fromIterable(()-> clients.values().iterator()).doOnNext(sessionHandler -> {
            sessionHandler.send(message);
        }).doOnError(System.out::println).subscribe();
    }

    /**
     * 获取在线的用户列表
     * @return
     */
    private List<String> getAllOnlinePlayers() {
        List<String> keys = new LinkedList<>();
        clients.entrySet().stream().forEach(e -> keys.add(e.getKey()));
        return  keys;
    }

}