package com.handwin.filestore;

import io.netty.buffer.ByteBufInputStream;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.DecoderResult;
import io.netty.handler.codec.MessageToMessageDecoder;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpMessage;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpMessage;
import io.netty.handler.codec.http.HttpObject;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.LastHttpContent;

import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.util.List;

import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.handwin.filestore.helper.ServerHelper;

public class StreamChunkAggregator extends MessageToMessageDecoder<HttpObject> {
	private static final Logger log = LoggerFactory.getLogger(StreamChunkAggregator.class);

    private volatile FullHttpMessage currentMessage;
    private volatile OutputStream out;
    private final int maxContentLength;
    private volatile File file;
    
    private ChannelHandlerContext ctx;
    
    public static final int DEFAULT_MAX_COMPOSITEBUFFER_COMPONENTS = 1024;
    private int maxCumulationBufferComponents = DEFAULT_MAX_COMPOSITEBUFFER_COMPONENTS;

    /**
     * Creates a new instance.
     */
    public StreamChunkAggregator(int maxContentLength) {
        this.maxContentLength = maxContentLength;
    }

	@Override
	protected void decode(ChannelHandlerContext ctx, HttpObject msg,
			List<Object> out) throws Exception {
		FullHttpMessage currentMessage = this.currentMessage;
		
		if (msg instanceof HttpMessage) {
			HttpMessage m = (HttpMessage) msg;
			if (msg instanceof HttpRequest) {
				HttpRequest header = (HttpRequest) msg;
                this.currentMessage = currentMessage = new DefaultFullHttpRequest(header.getProtocolVersion(),
                        header.getMethod(), header.getUri(), Unpooled.compositeBuffer(maxCumulationBufferComponents));
                
                final String localName = m.headers().get("file"); // 取上传文件名
                log.debug("upload file name is {}", localName);
                if(null == localName || "".equals(localName.trim())) {
                	ctx.fireChannelRead(m);
                }
                File dir = new File(ServerHelper.getDestDir().getAbsolutePath() + File.separator + ServerHelper.getStorePath(localName));
                if(!dir.exists())
                	dir.mkdirs();
                log.debug("upload file path is {}", dir.getAbsolutePath());
                File tempFile = new File(dir, localName + ".utmp");
                if(tempFile.exists()) { // 文件已经存在可能是上次上传遗留的
                	tempFile.delete();
                }
                this.file = tempFile;
                this.out = new FileOutputStream(file, true);
            } else {
                throw new Error();
            }

            currentMessage.headers().set(m.headers());
		} else if (msg instanceof HttpContent) {
			assert currentMessage != null;
			HttpContent chunk = (HttpContent) msg;
			
			if (chunk.content().isReadable()) {
                chunk.retain();
                IOUtils.copyLarge(new ByteBufInputStream(chunk.content()), this.out);
            }

            final boolean last;
            if (!chunk.getDecoderResult().isSuccess()) {
                currentMessage.setDecoderResult(
                        DecoderResult.failure(chunk.getDecoderResult().cause()));
                last = true;
            } else {
                last = chunk instanceof LastHttpContent;
            }

            if (last) {
            	this.out.flush();
                this.out.close();
                
                this.out = null;
                this.currentMessage = null;
                this.file = null;
                out.add(currentMessage);
            }
		} else {
            throw new Error();
        }
	}
	
	@Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        super.channelInactive(ctx);

        // release current message if it is not null as it may be a left-over
        if (currentMessage != null) {
            currentMessage.release();
            currentMessage = null;
        }
    }

    @Override
    public void handlerAdded(ChannelHandlerContext ctx) throws Exception {
        this.ctx = ctx;
    }

    @Override
    public void handlerRemoved(ChannelHandlerContext ctx) throws Exception {
        super.handlerRemoved(ctx);
        // release current message if it is not null as it may be a left-over as there is not much more we can do in
        // this case
        if (currentMessage != null) {
            currentMessage.release();
            currentMessage = null;
        }
    }


	/*protected void channelRead0(ChannelHandlerContext ctx, HttpMessage m)
			throws Exception {
		HttpMessage currentMessage = this.currentMessage;
        File localFile = this.file;
        if (currentMessage == null) {
            final String localName = m.headers().get("file"); // 取上传文件名
            log.debug("upload file name is {}", localName);
            if(null == localName || "".equals(localName.trim())) {
            	ctx.fireChannelRead(m);
            }
            this.currentMessage = m;
            File dir = new File(ServerHelper.getDestDir().getAbsolutePath() + File.separator + ServerHelper.getStorePath(localName));
            if(!dir.exists())
            	dir.mkdirs();
            log.debug("upload file path is {}", dir.getAbsolutePath());
            File tempFile = new File(dir, localName + ".utmp");
            if(tempFile.exists()) { // 文件已经存在可能是上次上传遗留的
            	tempFile.delete();
            }
            
            if (m instanceof HttpContent) {
                this.file = tempFile;
                this.out = new FileOutputStream(file, true);
            }
        } else {
            final HttpContent chunk = (HttpContent) m;
            if (chunk.content().isReadable()) {
            	chunk.retain();
            	IOUtils.copyLarge(new ByteBufInputStream(chunk.content()), this.out);
            }

            if (chunk instanceof LastHttpContent) {
                this.out.flush();
                this.out.close();

                currentMessage.headers().set(
                		HttpHeaders.Names.CONTENT_LENGTH,
                        String.valueOf(localFile.length()));

                this.out = null;
                this.currentMessage = null;
                this.file = null;
                
                ctx.fireChannelRead(m);
            }
        }
	}*/
}
