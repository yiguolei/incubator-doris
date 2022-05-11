package org.apache.doris.qe;

import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.doris.common.util.DebugUtil;
import org.apache.doris.plugin.AuditEvent;
import org.apache.doris.thrift.TResultBatch;
import org.apache.doris.thrift.TResultSinkType;
import org.apache.doris.thrift.TUniqueId;

import com.google.common.collect.Queues;

public class InternalQueryExecutor extends StmtExecutor {
    private static final int QUEUE_WAIT_SECONDS = 1; 
	private volatile BlockingQueue<TResultBatch> resultQueue = Queues.newLinkedBlockingDeque(10);
	private volatile boolean eos = false;
	private Throwable exception = null;
	private boolean isCancelled = false;
	
	public InternalQueryExecutor(ConnectContext context, String stmt) {
		super(context, stmt);
		context.setResultSinkType(TResultSinkType.ARROW_IPC_PROTOCOL);
		eos = false;
	}
	
    // query with a random sql
    public void execute() throws Exception {
    	// Start a new thread to execute the real query, and the thread will save the
    	// result batch to resultQueue.
    	// The thread that call execute(), should call getNext to pull resultbatch from
    	// result queue
    	new Thread(new Runnable() {
			@Override
			public void run() {
				try {
					InternalQueryExecutor.super.execute();
				} catch (Throwable t) {
					exception = t;
				}
			}
		}).run();
    }

	/**
	 * Get a result batch from queue, return null if the stream already finished
	 */
	public TResultBatch getNext() throws Exception {
		while (exception == null && !isCancelled) {
			TResultBatch res = resultQueue.poll(QUEUE_WAIT_SECONDS, TimeUnit.SECONDS);
			if (res != null) {
				return res;
			}
			if (eos) {
				if (resultQueue.isEmpty()) {
					break;
				}
			}
		}
		if (exception != null) {
			throw new Exception(exception);
		}
		return null;
	}
	
	/**
	 * If already pull as many records as need, but the result queue is still not empty
	 * then should call cancel to stop the running thread
	 */
	public void cancel() {
		isCancelled = true;
	}

    // Process a select statement.
    public void handleQueryStmt() throws Exception {
	    try {
	        QueryDetail queryDetail = new QueryDetail(context.getStartTime(),
	                DebugUtil.printId(context.queryId()),
	                context.getStartTime(), -1, -1,
	                QueryDetail.QueryMemState.RUNNING,
	                context.getDatabase(),
	                originStmt.originStmt);
	        context.setQueryDetail(queryDetail);
	        QueryDetailQueue.addOrUpdateQueryDetail(queryDetail);
	        RowBatch batch;
	        coord = new Coordinator(context, analyzer, planner);
	        QeProcessorImpl.INSTANCE.registerQuery(context.queryId(),
	                new QeProcessorImpl.QueryInfo(context, originStmt.originStmt, coord));
	        coord.exec();
	        while (!isCancelled) {
	            batch = coord.getNext();
	            // for outfile query, there will be only one empty batch send back with eos flag
	            if (batch.getBatch() != null) {
	            	resultQueue.offer(batch.getBatch(), QUEUE_WAIT_SECONDS, TimeUnit.SECONDS);
	            }
	            if (batch.isEos()) {
	            	eos = true;
	                break;
	            }
	        }
	        context.getState().setEof();
	    } catch (Exception e) {
	    	// If any exception occurs, set the exception to the memeber exception, so that the call thread
	    	// will get the real exception
	    	exception = e;
	    	throw e;
	    }
    }
}
