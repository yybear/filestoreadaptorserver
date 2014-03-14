package com.handwin.filestore;


import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.util.CharsetUtil;

import java.io.IOException;

import org.apache.commons.io.IOUtils;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.concurrent.FutureCallback;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.http.impl.nio.client.HttpAsyncClients;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.handwin.filestore.helper.ServerHelper;

/** 
 * 这边只是保存下上传的消息内容，然后将临时文件交给交给play处理
 * @author qgan
 * @version 2014年2月12日 上午9:15:44
 */
public class FileUploadAdaptorHandler extends SimpleChannelInboundHandler<DefaultFullHttpRequest> {
	private static final Logger log = LoggerFactory.getLogger(FileUploadAdaptorHandler.class);

	@Override
	protected void channelRead0(final ChannelHandlerContext ctx, DefaultFullHttpRequest msg) throws Exception {
		if(log.isDebugEnabled()) {
			log.debug("message received: begin");
		}

        final String filename = msg.headers().get("file"); 
        if(filename == null || "".equals(filename.trim())) { //没有文件名 直接返回4001 参数错误
        	String responseBody = "{\"result_code\": 4001,\"result_msg\": \"请求参数错误\"}";
        	response(responseBody.getBytes(), HttpResponseStatus.BAD_REQUEST, ctx);
            
        } else {
            // 转发给play服务处理
        	final CloseableHttpAsyncClient httpclient = HttpAsyncClients.createDefault();
        	httpclient.start();
        	try {
            	HttpGet request1 = new HttpGet(ServerHelper.getPlayServer());
            	request1.setHeader("Client-Session", msg.headers().get("client-session"));
            	request1.setHeader("Content-Range", msg.headers().get("content-range"));
            	request1.setHeader("file", msg.headers().get("file"));
            	httpclient.start();
            	httpclient.execute(request1, new FutureCallback<org.apache.http.HttpResponse>() {
					@Override
					public void failed(Exception e) {
						try {
							httpclient.close();
						} catch (IOException e1) {
							log.error(e1.getMessage(), e1);
						}
						serve500(ctx, filename);
					}
					
					@Override
					public void completed(org.apache.http.HttpResponse playResonse) {
						log.debug("HttpAsyncClient callback");
						int status = playResonse.getStatusLine().getStatusCode();
						log.debug("HttpAsyncClient callback playResonse status is {}", status);
						if(status != 200) {
							ServerHelper.deleteTmpFile(filename);
						}
						HttpEntity entity = playResonse.getEntity();
						byte[] bytes = new byte[(int) entity.getContentLength()];
						try {
							IOUtils.read(entity.getContent(), bytes);
							
							response(bytes, new HttpResponseStatus(status, ""), ctx);
						} catch (Exception e) {
							log.error(e.getMessage(), e);
							serve500(ctx, filename);
						} finally {
							try {
								httpclient.close();
							} catch (IOException e1) {
								log.error(e1.getMessage(), e1);
							}
						}
					}
					
					@Override
					public void cancelled() {
						try {
							httpclient.close();
						} catch (IOException e1) {
							log.error(e1.getMessage(), e1);
						}
						serve500(ctx, filename);
					}
				});
        	} catch (Exception e) {
        		httpclient.close();
        		log.error(e.getMessage(), e);
        		serve500(ctx, filename);
        	}
        }
        
        if(log.isDebugEnabled()) {
			log.debug("message received: end");
		}
		
	}
	
	public static void serve500(ChannelHandlerContext ctx, String filename) {
		if(log.isDebugEnabled()) {
			log.debug("serve500: start");
		}
		
		ServerHelper.deleteTmpFile(filename);

		FullHttpResponse response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.INTERNAL_SERVER_ERROR, Unpooled.copiedBuffer("Failure: " + HttpResponseStatus.INTERNAL_SERVER_ERROR.toString() + "\r\n", CharsetUtil.UTF_8));
		response.headers().set(HttpHeaders.Names.CONTENT_TYPE, "text/plain; charset=UTF-8");
		ctx.writeAndFlush(response).addListener(ChannelFutureListener.CLOSE);
        
        if(log.isDebugEnabled()) {
        	log.debug("serve500: end");
        }
    }
	
	public static void response(byte[] body, HttpResponseStatus status, ChannelHandlerContext ctx) {
		if(log.isDebugEnabled()) {
			log.debug("response: start");
		}
		
		FullHttpResponse response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, status, Unpooled.copiedBuffer("Successful: " + status.toString() + "\r\n", CharsetUtil.UTF_8));
		response.headers().set(HttpHeaders.Names.CONTENT_TYPE, "text/plain; charset=UTF-8");
		response.content().writeBytes(body);
		ctx.writeAndFlush(response).addListener(ChannelFutureListener.CLOSE);
        
        if(log.isDebugEnabled()) {
        	log.debug("response: end");
        }
	}
}
