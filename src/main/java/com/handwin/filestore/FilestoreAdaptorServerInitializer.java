package com.handwin.filestore;

import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.http.HttpRequestDecoder;
import io.netty.handler.codec.http.HttpResponseEncoder;

/**
 * @author qgan
 * @version 2014年2月12日 上午9:58:29
 */
public class FilestoreAdaptorServerInitializer extends
		ChannelInitializer<SocketChannel> {

	@Override
	protected void initChannel(SocketChannel ch) throws Exception {
		ChannelPipeline pipeline = ch.pipeline();
		pipeline.addLast("decoder", new HttpRequestDecoder());
		pipeline.addLast("aggregator", new StreamChunkAggregator(-1));
		pipeline.addLast("encoder", new HttpResponseEncoder());
		pipeline.addLast("handler", new FileUploadAdaptorHandler());
	}
}
