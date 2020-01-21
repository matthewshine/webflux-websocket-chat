package sample.webflux.websocket.netty.handler;

import java.nio.channels.ClosedChannelException;
import java.util.*;

import org.springframework.util.StringUtils;
import org.springframework.web.reactive.socket.HandshakeInfo;
import org.springframework.web.reactive.socket.WebSocketSession;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;
import reactor.core.publisher.ReplayProcessor;
import reactor.core.scheduler.Schedulers;
import reactor.ipc.netty.channel.AbortedException;

/**
 * WebsessionHandler解读
 *
 * session控制的封装类，封装了connect状态，disconnect状态，clientId,WebsocketSession等信息
 * 注：reactive里面重要思想就是，建立管道，订阅管道。不按代码顺序执行，而是看事件触发的点和管道流向。
 *
 */
public class WebSocketSessionHandler 
{
	private final ReplayProcessor<String> receiveProcessor;
	private final MonoProcessor<WebSocketSession> connectedProcessor; //processor为了封装成hot stream
	private final MonoProcessor<WebSocketSession> disconnectedProcessor;
	private boolean webSocketConnected;
	private String  clientId;
	private WebSocketSession session;
	
	public WebSocketSessionHandler()
	{
		this(50);
	}
	
	public WebSocketSessionHandler(int historySize)
	{
		receiveProcessor = ReplayProcessor.create(historySize);
		connectedProcessor = MonoProcessor.create();
		disconnectedProcessor = MonoProcessor.create();
		
		webSocketConnected = false;
	}
	
	protected Mono<Void> handle(WebSocketSession session)
	{
		//获取到session后设置全局变量
		this.session = session;
		HandshakeInfo handshakeInfo = session.getHandshakeInfo();
		Map<String, String> queryMap = getQueryMap(handshakeInfo.getUri().getQuery());
		queryMap.forEach((k,v)-> System.out.println(k+":"+v));
		String clientId = queryMap.getOrDefault("clientId", "defaultId");
		this.clientId =clientId;
		System.out.println("========================"+clientId);
		//以上都是在建立连接时才执行，收发消息不执行

		/**
		 * receive,connected,disconnected,只是建立了管道，具体的处理是在subcribe的地方，就是websockethandler中
		 * onNext就是把消息发送道管道，订阅该管道的websocket会处理
		 */

		//定义接收到消息的处理器//收到消息后转发给receiveProcessor
		Flux<String> receive =
			session
				.receive()
				.map(message -> message.getPayloadAsText())
				.doOnNext(textMessage -> receiveProcessor.onNext(textMessage))
				.doOnComplete(() -> receiveProcessor.onComplete());

		//建立连接时，设置flag并且推送session到处理器
		Mono<Object> connected =
				Mono
					.fromRunnable(() -> 
					{
						webSocketConnected = true;
						connectedProcessor.onNext(session);
					});
		//断开连接时，设置flag并且推送session到处理器。同时关闭消息接收
		Mono<Object> disconnected =
				Mono
					.fromRunnable(() -> 
					{
						webSocketConnected = false;
						disconnectedProcessor.onNext(session);
					})
					.doOnNext(value -> receiveProcessor.onComplete());
		//执行connected,激活receive消息，当消息（连接关闭or异常）complete时，执行disconnected逻辑
		// then() - 当receive结束的时候，返回一个Mono<Void>
		return connected.thenMany(receive).then(disconnected).then();
	}
	
	public Mono<WebSocketSession> connected()
	{
		return connectedProcessor;
	}
	
	public Mono<WebSocketSession> disconnected()
	{
		return disconnectedProcessor;
	}
	
	public boolean isConnected()
	{
		return webSocketConnected;
	}
	
	public Flux<String> receive()
	{
		return receiveProcessor;
	}
	
	public void send(String message)
	{
		if (webSocketConnected)
		{
			session
				.send(Mono.just(session.textMessage(message)))
				.doOnError(ClosedChannelException.class, t -> connectionClosed())
				.doOnError(AbortedException.class, t -> connectionClosed())
				.onErrorResume(ClosedChannelException.class, t -> Mono.empty())
				.onErrorResume(AbortedException.class, t -> Mono.empty())
				.subscribe();
		}	
	}
	
	private void connectionClosed()
	{
		if (webSocketConnected)
		{
			webSocketConnected = false;
			disconnectedProcessor.onNext(session);
		}
	}

	private Map<String, String> getQueryMap(String queryStr) {
		Map<String, String> queryMap = new HashMap<>();
		if (!StringUtils.isEmpty(queryStr)) {
			String[] queryParam = queryStr.split("&");
			Arrays.stream(queryParam).forEach(s -> {
				String[] kv = s.split("=", 2);
				String value = kv.length == 2 ? kv[1] : "";
				queryMap.put(kv[0], value);
			});
		}
		return queryMap;
	}

	public String getClientId() {
		return clientId;
	}

	public void setClientId(String clientId) {
		this.clientId = clientId;
	}
}