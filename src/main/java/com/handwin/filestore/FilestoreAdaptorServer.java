package com.handwin.filestore;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.http.HttpRequestDecoder;
import io.netty.handler.codec.http.HttpResponseEncoder;
import io.netty.util.concurrent.DefaultThreadFactory;

import java.io.File;
import java.io.FileInputStream;
import java.net.InetSocketAddress;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.handwin.filestore.helper.ServerHelper;

/** 
 * @author qgan
 * @version 2014年2月12日 上午9:03:23
 */
public class FilestoreAdaptorServer {
	private static final Logger log = LoggerFactory.getLogger(FilestoreAdaptorServer.class);
    
	public void start() {
		int httpPort = ServerHelper.getPort(9800); // 从配置中读取
		assert (httpPort != -1);
		
		EventLoopGroup bossGroup = new NioEventLoopGroup(0, new DefaultThreadFactory("NETTY-BOSS"));
		EventLoopGroup workGroup = new NioEventLoopGroup(0, new DefaultThreadFactory("NETTY-WORKER"));
		try {
			ServerBootstrap bootstrap = new ServerBootstrap();
			bootstrap.group(bossGroup, workGroup)
				.channel(NioServerSocketChannel.class)
				.localAddress(new InetSocketAddress(httpPort))
	            .option(ChannelOption.SO_KEEPALIVE, true)
	            .option(ChannelOption.SO_REUSEADDR, true)
	            .childHandler(new FilestoreAdaptorServerInitializer());
			
			log.debug("server start");
			ChannelFuture f = bootstrap.bind(new InetSocketAddress(httpPort)).sync();
			f.channel().closeFuture().sync();
		} catch (InterruptedException e) {
			log.error("run error", e);
		} finally {
            bossGroup.shutdownGracefully();
            workGroup.shutdownGracefully();
        }
        
		
		/*bootstrap.setPipelineFactory(new FilestoreAdaptorServerPipelineFactory());
        bootstrap.bind(new InetSocketAddress(httpPort));
        bootstrap.setOption("child.tcpNoDelay", true);*/
	}
	
	public static void main(String[] args) {
		if(args == null) {
			System.err.println("need server config file!");
			System.exit(1);
		}
		
		File serverConfFile = new File(args[0]);
		//File serverConfFile = new File("D:\\workset\\filestoreadaptorserver\\server.conf");
		try {
			ServerHelper.SERVER_CONF.load(new FileInputStream(serverConfFile));
		} catch (Exception e) {
			e.printStackTrace();
			System.err.println("load server conf error!");
			System.exit(1);
		}
		/*ch.qos.logback.classic.Logger root = (ch.qos.logback.classic.Logger)LoggerFactory.getLogger("com.handwin.filestrore");
		Level level = ServerHelper.getLogLevel();
		if(level != null)
			root.setLevel(level);*/
		
		FilestoreAdaptorServer server = new FilestoreAdaptorServer();
		server.start();
	}
	
}
